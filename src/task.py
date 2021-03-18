import argparse
import itertools
import json
import multiprocessing
import os
import re
import shutil
import subprocess
import tarfile
import threading
import uuid
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, TextIO, Tuple, Union

import requests
from google.cloud.storage import Client  # type: ignore
from osgeo import gdal, gdalconst  # type: ignore
import raster_utils
from task_base import HIITask  # type: ignore
from timer import Timer
import config

gdal.SetConfigOption("GDAL_CACHEMAX", "512")
gdal.SetConfigOption("GDAL_SWATH_SIZE", "512")
gdal.SetConfigOption("GDAL_MAX_DATASET_POOL_SIZE", "400")


def run_in_thread(fn):
    def run(*k, **kw):
        t = threading.Thread(target=fn, args=k, kwargs=kw)
        t.start()
        return
    return run


def _rasterize(in_file: Union[str, Path], output_path: Union[str, Path], output_bounds: Optional[List[float]]=None) -> Path:
    output_bounds = output_bounds or [-180.0, -90.0, 180.0, 90.0]
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
    3. Split up text file by attribute and tags and 1 million row CSV files
    4. Rasterize each CSV file.
    5. Merge all tiff into 1 multiband tiff file
    6. Upload to Google Storage
    7. Clean up working directories and files.

    """

    ee_osm_root = "osm"
    google_creds_path = "/.google_creds"
    _asset_prefix = f"projects/{HIITask.ee_project}/{ee_osm_root}"

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        self._args = kwargs
        self.max_rows = 1000000

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

    def _create_file(self, directory: str, attributes_tag: str) -> Tuple[Path, TextIO]:
        name = f"{attributes_tag}_{uuid.uuid4()}.csv"
        path = Path(directory, name)
        f = open(path, "w")
        f.write('"WKT","BURN"\n')
        return path, f

    def _parse_row(self, row: str) -> Tuple[str, str]:
        idx = row.rindex(")") + 1
        attr_tags = row[idx + 1 : -1].split(",")

        if not attr_tags or not attr_tags[0]:
            return None, None

        # wkt, attribute tags
        return row[0:idx], attr_tags

    def _create_image_metadata(
        self,
        image_paths: List[Union[str, Path]],
        output_image_uris: List[str],
        road_uri: str,
        output_file: Union[str, Path]
    ) -> Path:
        bands_metadata = dict()
        for n, img_pth in enumerate(image_paths):
            name = Path(os.path.splitext(img_pth)[0]).name
            attribute_tag = name.rsplit("_", 1)[0]
            attribute, tag = attribute_tag.split("=")
            if attribute_tag not in bands_metadata:
                bands_metadata[attribute_tag] = dict(
                    attribute=attribute,
                    tag=tag,
                    bands=[]
                )
            bands_metadata[attribute_tag]["bands"].append(n + 1)

        metadata = dict(
            bands=bands_metadata,
            images=[str(oip) for oip in output_image_uris],
            road=road_uri
        )

        with open(output_file, "w") as f:
            f.write(json.dumps(metadata, indent=2))
        
        return output_file

    @run_in_thread
    def _backup_step_data(self, file_paths: Union[str, Path], backup_name: Union[str, Path]):
        if self._args["backup_step_data"] is False:
            return

        if isinstance(file_paths, (Path, str,)):
            file_paths = [file_paths]
        
        tar_name = f"{backup_name}.tar.gz"
        backup_path = Path(self._working_directory, tar_name)
        with tarfile.open(backup_path, "w:gz") as tar:
            for f in file_paths:
                tar.add(str(f))

        self.upload_to_cloudstorage(backup_path, tar_name)

    # Step 1
    def download_osm(self, osm_url: str, osm_file_path: Union[str, Path]) -> Path:
        with requests.get(osm_url, stream=True) as r:
            with open(osm_file_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)

        return osm_file_path

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
    def split_osmium_text_file(
        self, txt_file: str, output_dir: Dict[str, TextIO], roads_file_path: Union[str, Path]
    ) -> Tuple[List[Path], Path]:
        file_indicies = defaultdict(list)
        file_handlers = dict()

        if Path(output_dir).exists() is False:
            Path(output_dir).mkdir(exist_ok=True)

        max_rows = self.max_rows - 1
        _parse_row = self._parse_row
        _create_file = self._create_file
        output_files = []
        roads_file = open(roads_file_path, "w")
        roads_tags = {rt: rt.split("=") for rt in config.road_tags}
        roads_file.write('"wkt","attribute","tag"\n')
        with open(txt_file, "r") as fr:
            for row in fr:
                wkt, attribute_tags = _parse_row(row)

                if wkt is None:
                    continue

                for attribute_tag in attribute_tags:
                    if (
                        attribute_tag not in file_handlers
                        or file_indicies[attribute_tag] >= max_rows
                    ):
                        path, handle = _create_file(output_dir, attribute_tag)
                        output_files.append(path)
                        file_handlers[attribute_tag] = handle
                        file_indicies[attribute_tag] = 0

                    file_handlers[attribute_tag].write(f'"{wkt}",\n')
                    file_indicies[attribute_tag] += 1

                    if attribute_tag in roads_tags:
                        roads_file.write(f'"{wkt}","{roads_tags[attribute_tag][0]}","{roads_tags[attribute_tag][1]}"\n')
        
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

        num_cpus = multiprocessing.cpu_count() - 1 or 1
        with ProcessPoolExecutor(max_workers=num_cpus) as executor:
            results = executor.map(
                _rasterize,
                csv_files,
                output_files,
                itertools.repeat(self.bounds)
            )
            for result in results:
                if isinstance(result, Exception):
                    raise result

        return output_files

    # Step 5
    def stack_images(
        self, image_paths: List[Union[str, Path]], output_dir: Union[str, Path]
    ) -> List[Path]:
        
        num_cpus = multiprocessing.cpu_count() - 1 or 1
        output_image_paths = [Path(output_dir, f"stacked-{n+1}.tif") for n in range(num_cpus)]
        img_stack_metas = raster_utils.split_image(image_paths, num_cpus, window_size=1024)

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
    def upload_to_cloudstorage(self, src_path: Union[str, Path], name: Optional[str] = None) -> str:
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
                    self._unique_file_name(ext="txt", prefix=f"pbf_text-{self.taskdate}")
                )

        with Timer("Split text file to CSV files"):
            csv_files, road_file_path = self.split_osmium_text_file(
                osmium_text_file,
                Path(self._working_directory, "split_files"),
                Path(self._working_directory, "roads.csv"),
            )

        with Timer("Rasterize CSV files"):
            image_paths = self.rasterize(
                csv_files, Path(self._working_directory, "images")
            )

        with Timer("Many images to multi-bands image"):
            stacked_images = self.stack_images(
                image_paths, self._working_directory
            )

        with Timer("Upload tiff to GS"):
            image_uris = []
            for stacked_image in stacked_images:
                image_uris.append(self.upload_to_cloudstorage(stacked_image))
            road_text_uri = self.upload_to_cloudstorage(road_file_path)

            metadata_file = self._create_image_metadata(
                image_paths,
                image_uris,
                road_text_uri,
                Path(self._working_directory, "metadata.json")
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
        help=(
            "Add local path to OSM source file."
            " If not provided, file will be downloaded"
        ),
    )
    parser.add_argument(
        "-u",
        "--osm_url",
        type=str,
        help="Set a different source url to download OSM pbf file.",
    )

    parser.add_argument(
        "--osmium_text_file",
        type=str,
        help="Text file created from osmium export.",
    )

    parser.add_argument(
        "-w",
        "--working_dir",
        type=str,
        help="Working directory to store files and directories during processing."
    )

    parser.add_argument(
        "--extent",
        type=str,
        help="Output geographic bounds."
    )

    parser.add_argument(
        "--backup_step_data",
        action="store_true",
        help="Backup up working data to Google Cloud Storage",
    )

    options = parser.parse_args()
    task = HIIOSMRasterize(**vars(options))
    task.run()
