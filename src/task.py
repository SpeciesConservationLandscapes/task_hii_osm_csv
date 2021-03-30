import argparse
import itertools
import json
import math
import multiprocessing
import os
import re
import shutil
import subprocess
import tarfile
import threading
import uuid
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO, Tuple, Union

import requests
from google.cloud.storage import Client  # type: ignore
from osgeo import gdal, gdalconst  # type: ignore
from pyproj import Geod  # type: ignore
from shapely import wkt as shp_wkt  # type: ignore
from shapely.validation import explain_validity  # type: ignore
from task_base import HIITask  # type: ignore

import raster_utils
from timer import Timer

gdal.SetConfigOption("GDAL_CACHEMAX", "512")
gdal.SetConfigOption("GDAL_SWATH_SIZE", "512")
gdal.SetConfigOption("GDAL_MAX_DATASET_POOL_SIZE", "400")


def run_in_thread(fn):
    def run(*k, **kw):
        t = threading.Thread(target=fn, args=k, kwargs=kw)
        t.start()
        return

    return run


def _rasterize(
    in_file: Union[str, Path],
    output_path: Union[str, Path],
    output_bounds: List[float],
) -> Path:
    opts = gdal.RasterizeOptions(
        format="GTiff",
        outputType=gdalconst.GDT_Byte,
        noData=0,
        initValues=0,
        burnValues=1,
        xRes=0.003,
        yRes=0.003,
        targetAlignedPixels=True,
        outputSRS="EPSG:4326",
        outputBounds=output_bounds,
        creationOptions=[
            "TILED=YES",
            "BLOCKXSIZE=1024",
            "BLOCKYSIZE=1024",
        ],
    )
    gdal.Rasterize(str(output_path), str(in_file), options=opts)

    return Path(output_path)


class ConversionException(Exception):
    pass


class HIIOSMRasterize(HIITask):
    """

    Process:

    1. Fetch OSM pbf file
    2. Convert PBF file -> Text file (filter by attribute/tag list)
    3.
        a) Split up text file into one CSV file per attribute/tag combination per 1 million rows
        b) Clean geometry and write out road tags to a roads CSV file
    4. Rasterize each CSV file
    5. Merge all tiff images into 1 multiband tiff file and split image
    6. Upload to Google Storage
    7. Clean up working directories and files

    """

    ee_osm_root = "osm"
    google_creds_path = "/.google_creds"
    _asset_prefix = f"projects/{HIITask.ee_project}/{ee_osm_root}"
    config_json = Path(Path(__file__).parent.absolute(), "osmium_config.json")

    MIN_GEOM_AREA = 5  # in meters
    POLYGON_PRECISION = 5
    MAX_ROWS = 1000000

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        self._args = kwargs

        _extent = self._args.get("extent")
        if _extent:
            self.bounds = [float(c) for c in self._args["extent"].split(",")]
        else:
            self.bounds = self.extent[0] + self.extent[2]

        creds_path = Path(self.google_creds_path)
        self.service_account_key = os.environ["SERVICE_ACCOUNT_KEY"]
        if creds_path.exists() is False:
            with open(creds_path, "w") as f:
                f.write(self.service_account_key)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_creds_path

    def _unique_file_name(self, ext: str, prefix: Optional[str] = None) -> str:
        name = f"{uuid.uuid4()}.{ext}"
        if prefix:
            name = f"{prefix}-{name}"

        return name

    def _parse_task_id(self, output: Union[str, bytes]) -> Optional[str]:
        if isinstance(output, bytes):
            text = output.decode("utf-8")
        else:
            text = output

        task_id_regex = re.compile(r"(?<=ID: ).*", flags=re.IGNORECASE)
        try:
            matches = task_id_regex.search(text)
            if matches is None:
                return None
            return matches[0]
        except TypeError:
            return None

    def _create_file(
        self, directory: Union[str, Path], attributes_tag: str
    ) -> Tuple[Path, TextIO]:
        name = f"{attributes_tag}_{uuid.uuid4()}.csv"
        path = Path(directory, name)
        f = open(path, "w")
        f.write('"WKT","BURN"\n')
        return path, f

    def _parse_row(self, row: str) -> Union[Tuple[str, List[str]], Tuple[None, None]]:
        idx = row.rindex(")") + 1
        attr_tags = row[idx + 1:-1].split(",")

        if not attr_tags or not attr_tags[0]:
            return None, None

        # wkt, attribute tags
        return row[0:idx], attr_tags

    def _create_image_metadata(
        self,
        image_paths: List[Union[str, Path]],
        output_image_uris: List[str],
        road_uri: str,
        output_file: Union[str, Path],
    ) -> Path:
        bands_metadata: Dict[str, Any] = dict()
        for n, img_pth in enumerate(image_paths):
            name = Path(os.path.splitext(img_pth)[0]).name
            attribute_tag = name.rsplit("_", 1)[0]
            attribute, tag = attribute_tag.split("=")
            if attribute_tag not in bands_metadata:
                bands_metadata[attribute_tag] = dict(
                    attribute=attribute, tag=tag, bands=[]
                )
            bands_metadata[attribute_tag]["bands"].append(n + 1)

        metadata = dict(
            bands=bands_metadata,
            images=[str(oip) for oip in output_image_uris],
            road=road_uri,
        )

        with open(output_file, "w") as f:
            f.write(json.dumps(metadata, indent=2))

        return Path(output_file)

    def _clean_geometry(  # noqa: C901
        self, wkt: Optional[str], geod: Geod, fail_fast: bool = False
    ) -> Optional[str]:
        if not wkt or "POLYGON" not in wkt:
            return wkt

        geom = shp_wkt.loads(
            shp_wkt.dumps(shp_wkt.loads(wkt), rounding_precision=self.POLYGON_PRECISION)
        ).simplify(0)
        if geom.is_valid is False:
            if fail_fast is True:
                print(f"INVALID [{explain_validity(geom)}] - {geom}")
                return None
            # Try validating one more time after attempting to clean with buffer
            return self._clean_geometry(shp_wkt.dumps(geom.buffer(0)), geod, True)
        elif geom.is_empty is True:
            print(f"EMPTY - {geom}")
            return None

        area = abs(geod.geometry_area_perimeter(geom)[0])
        if area < self.MIN_GEOM_AREA:
            print(f"AREA[{area}] - {geom}")
            return None

        return shp_wkt.dumps(geom, rounding_precision=self.POLYGON_PRECISION)

    @run_in_thread
    def _backup_step_data(
        self, file_paths: Union[str, Path, list], backup_name: Union[str, Path]
    ):
        if self._args["backup_step_data"] is False:
            return

        if isinstance(
            file_paths,
            (
                Path,
                str,
            ),
        ):
            file_paths = [file_paths]

        tar_name = f"{backup_name}.tar.gz"
        backup_path = Path(self._working_directory, tar_name)
        with tarfile.open(backup_path, "w:gz") as tar:
            for f in file_paths:
                tar.add(str(f))

        self.upload_to_cloudstorage(backup_path, tar_name)

    def _get_roads_tags(self) -> Dict[str, Tuple[str, str]]:
        with open(self.config_json, "r") as f:
            config = json.load(f)
            return config["road_tags"]

    # Step 1
    def download_osm(self, osm_url: str, osm_file_path: Union[str, Path]) -> Path:
        with requests.get(osm_url, stream=True) as r:
            with open(osm_file_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)

        return Path(osm_file_path)

    # Step 2
    def osm_to_txt(
        self, osm_file_path: Union[str, Path], txt_file_path: Union[str, Path]
    ) -> Path:
        try:
            cmd = [
                "/usr/bin/osmium",
                "export",
                "-f text",
                "-c osmium_config.json",
                "-O",
                f"-o {(txt_file_path)}",
                str(osm_file_path),
            ]

            subprocess.check_output(" ".join(cmd), stderr=subprocess.STDOUT, shell=True)
            return Path(txt_file_path)
        except subprocess.CalledProcessError as err:
            raise ConversionException(err.stdout)

    # Step 3
    def split_osmium_text_file(  # noqa: C901
        self,
        txt_file: str,
        output_dir: Union[str, Path],
        roads_file_path: Union[str, Path],
        roads_tags: Dict[str, Tuple[str, str]]
    ) -> Tuple[List[Path], Path]:
        file_indicies: Dict[str, int] = dict()
        file_handlers = dict()

        if Path(output_dir).exists() is False:
            Path(output_dir).mkdir(exist_ok=True)

        geod = Geod(ellps="WGS84")
        max_rows = self.MAX_ROWS - 1
        _parse_row = self._parse_row
        _create_file = self._create_file
        output_files = []
        roads_file = open(roads_file_path, "w")
        roads_file.write('"wkt","attribute","tag"\n')
        with open(txt_file, "r") as fr:
            for row in fr:
                wkt, attribute_tags = _parse_row(row)

                if wkt is None or attribute_tags is None:
                    continue

                for attr_tag in attribute_tags:
                    if (attr_tag not in file_handlers or file_indicies[attr_tag] >= max_rows):
                        path, handle = _create_file(output_dir, attr_tag)
                        output_files.append(path)
                        file_handlers[attr_tag] = handle
                        file_indicies[attr_tag] = 0

                    file_handlers[attr_tag].write(f'"{wkt}",\n')
                    file_indicies[attr_tag] += 1

                    if attr_tag in roads_tags:
                        rd_attr_tag = roads_tags[attr_tag]
                        wkt = self._clean_geometry(wkt, geod)
                        if wkt is not None:
                            roads_file.write(
                                f'"{wkt}","{rd_attr_tag[0]}","{rd_attr_tag[1]}"\n'
                            )

        roads_file.close()

        return output_files, Path(roads_file_path)

    # Step 4
    def rasterize(
        self, csv_files: List[Union[str, Path]], output_dir: Union[str, Path]
    ) -> List[Path]:
        if Path(output_dir).exists() is False:
            Path(output_dir).mkdir(exist_ok=True)

        output_files = [
            Path(output_dir, f"{Path(os.path.splitext(f)[0]).name}.tif")
            for f in csv_files
        ]

        bounds = self.bounds
        num_cpus = multiprocessing.cpu_count() - 1 or 1
        with ProcessPoolExecutor(max_workers=num_cpus) as executor:
            results = executor.map(
                _rasterize, csv_files, output_files, itertools.repeat(bounds)
            )
            for result in results:
                if isinstance(result, Exception):
                    raise result

        return output_files

    # Step 5
    def stack_images(
        self, image_paths: List[Union[str, Path]], output_dir: Union[str, Path]
    ) -> List[Path]:

        num_cpus = math.ceil((multiprocessing.cpu_count() - 1) / 2.0) or 1
        output_image_paths = [
            Path(output_dir, f"stacked-{n+1}.tif") for n in range(num_cpus)
        ]
        img_stack_metas = raster_utils.split_image(
            image_paths, num_cpus, window_size=1024
        )

        with ProcessPoolExecutor(max_workers=num_cpus) as executor:
            results = executor.map(
                raster_utils.stack_images,
                img_stack_metas,
                output_image_paths,
            )
            for result in results:
                if isinstance(result, Exception):
                    raise result

        return output_image_paths

    # Step 6
    def upload_to_cloudstorage(
        self, src_path: Union[str, Path], name: Optional[str] = None
    ) -> str:
        targ_name = name or Path(src_path).name
        targ_path = Path(str(self.taskdate), targ_name)
        client = Client()
        bucket = client.bucket(os.environ["HII_OSM_BUCKET"])
        blob = bucket.blob(str(targ_path))
        blob.upload_from_filename(str(src_path), timeout=3600)

        return f"gs://{os.environ['HII_OSM_BUCKET']}/{targ_path}"

    # Step 7
    def cleanup_working_files(self):
        print("Not Implemented")

    def calc(self):
        self._working_directory = self._args.get("working_dir") or "/tmp"
        roads_tags = self._get_roads_tags()

        osmium_text_file = self._args.get("osmium_text_file")
        osm_file = self._args.get("osm_file")
        if osm_file is None and osmium_text_file is None:
            with Timer("Download osm file"):
                osm_url = self._args.get("osm_url") or os.environ["OSM_DATA_SOURCE"]
                file_path = Path(
                    self._working_directory, self._unique_file_name(ext="pbf")
                )
                osm_file = self.download_osm(osm_url, file_path)

        if osmium_text_file is None:
            with Timer("Convert OSM to text file"):
                osmium_text_file = Path(
                    self._working_directory, self._unique_file_name(ext="txt")
                )
                osmium_text_file = self.osm_to_txt(osm_file, osmium_text_file)
                self._backup_step_data(
                    osmium_text_file,
                    self._unique_file_name(
                        ext="txt", prefix=f"pbf_text-{self.taskdate}"
                    ),
                )

        with Timer("Split text file to CSV files"):
            csv_files, road_file_path = self.split_osmium_text_file(
                osmium_text_file,
                Path(self._working_directory, "split_files"),
                Path(self._working_directory, "roads.csv"),
                roads_tags
            )

        with Timer("Rasterize CSV files"):
            image_paths = self.rasterize(
                csv_files, Path(self._working_directory, "images")
            )

        with Timer("Many images to multi-bands image"):
            stacked_images = self.stack_images(image_paths, self._working_directory)

        with Timer("Upload tiff to GS"):
            image_uris = []
            for stacked_image in stacked_images:
                image_uris.append(self.upload_to_cloudstorage(stacked_image))
            road_text_uri = self.upload_to_cloudstorage(road_file_path)

            metadata_file = self._create_image_metadata(
                image_paths,
                image_uris,
                road_text_uri,
                Path(self._working_directory, "metadata.json"),
            )
            gs_metadata_uri = self.upload_to_cloudstorage(metadata_file)
            print(f"Metadata uri: {gs_metadata_uri}")
            print("Image URIS:")
            for image_uri in image_uris:
                print(f"\t{image_uri}")
            print(f"Road uri: {road_text_uri}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--taskdate", default=datetime.now(timezone.utc).date())
    parser.add_argument(
        "-f",
        "--osm_file",
        type=str,
        default=None,
        help=(
            "Add local path to OSM source file."
            " If not provided, file will be downloaded"
        ),
    )
    parser.add_argument(
        "-u",
        "--osm_url",
        type=str,
        default="https://ftp.fau.de/osm-planet/pbf/planet-latest.osm.pbf",
        help="Set a different source url to download OSM pbf file.",
    )

    parser.add_argument(
        "--osmium_text_file",
        type=str,
        default=None,
        help="Text file created from osmium export.",
    )

    parser.add_argument(
        "-w",
        "--working_dir",
        type=str,
        default="/tmp",
        help="Working directory to store files and directories during processing.",
    )

    parser.add_argument(
        "--extent",
        type=str,
        default="-180.0,-58.0,180.0,84.0",
        help="Output geographic bounds."
    )

    parser.add_argument(
        "--backup_step_data",
        action="store_true",
        default=False,
        help="Backup up osm to text file to Google Cloud Storage",
    )

    options = parser.parse_args()
    task = HIIOSMRasterize(**vars(options))
    task.run()
