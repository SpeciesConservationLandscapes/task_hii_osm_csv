import argparse
import json
import multiprocessing
import os
import shutil
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, TextIO

import requests
from google.cloud.storage import Client  # type: ignore
from pyproj import Geod
from shapely import wkt as shp_wkt  # type: ignore

# from shapely.validation import explain_validity
from task_base import HIITask  # type: ignore


class ConversionException(Exception):
    pass


class HIIOSMCSV(HIITask):
    """

    Process:

    1. Fetch OSM pbf file
    2. Convert PBF file -> Text file (filter by attribute/tag list)
    3. Split up text file by attribute and tags into CSV files
        -> cleans and validates geometry
    4. Using CSV files to Google Storage

    """

    ee_osm_root = "osm"
    google_creds_path = "/.google_creds"
    MIN_GEOM_AREA = 5  # in meters
    POLYGON_PRECISION = 5

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        self._args = kwargs
        if "osm_file" in self._args:
            self.osm_file = self._args["osm_file"]

        self.csv_file = self._args.get("csv_file")
        self.osm_url = self._args.get("osm_url") or os.environ["OSM_DATA_SOURCE"]
        creds_path = Path(self.google_creds_path)
        if creds_path.exists() is False:
            with open(str(creds_path), "w") as f:
                f.write(self.service_account_key)

    @property
    def directory(self):
        return f"/app/tmp/{self.taskdate}"

    def _unique_file_name(self, ext: str, prefix: Optional[str] = None) -> str:
        name = f"{uuid.uuid4()}.{ext}"
        if prefix:
            name = f"{prefix}-{name}"

        return name

    def _get_asset_id(self, attribute, tag, task_date):
        root = f"projects/{self.ee_project}/{self.ee_osm_root}"
        return f"{root}/{attribute}/{tag}/{tag}_{task_date}"

    def _upload_to_cloudstorage(self, src_path: str) -> str:
        targ_path = Path(str(self.taskdate), Path(src_path).name)
        client = Client()
        bucket = client.bucket(os.environ["HII_OSM_BUCKET"])
        blob = bucket.blob(str(targ_path))
        blob.upload_from_filename(src_path, timeout=3600)

        return str(targ_path)

    def _get_tags(self) -> List[List[str]]:
        with open("config.json", "r") as fr:
            config = json.loads(fr.read())
            return [at.split("=") for at in config.get("include_tags") or []]

    def _create_files(
        self, directory: str, attributes_tags: List[tuple]
    ) -> Dict[str, TextIO]:
        files = {}
        for attribute, tag in attributes_tags:
            files[f"{attribute}={tag}"] = open(
                Path(directory, f"{attribute}_{tag}.csv"), "w"
            )
            files[f"{attribute}={tag}"].write('"WKT","tag","burn"\n')
        return files

    def _close_files(self, files: Dict[str, TextIO]):
        for f in files.values():
            if f and f.closed is False:
                f.close()

    def _unlink(self, path: str):
        # Doing it this way for backwards compatibility for python
        # versions less than v3.8
        try:
            Path(path).unlink()
        except FileNotFoundError:
            pass

    def _get_row_attribute_tag(
        self, attribute_tag: str, attributes_tags: List[List[str]]
    ) -> Optional[List[str]]:  # noqa: C901
        try:
            if "=" not in attribute_tag:
                return None
            elif "," not in attribute_tag:
                return [t.strip() for t in attribute_tag.split("=")]
            else:
                _attribute_tags = attribute_tag.split(",")
                for tags in _attribute_tags:
                    attr_tag = self._get_row_attribute_tag(tags, attributes_tags)
                    if attr_tag is not None and attr_tag in attributes_tags:
                        return attr_tag
            return None
        except (TypeError, ValueError):
            return None

    def download_osm(self) -> str:
        file_path = self._unique_file_name(ext="pbf")

        with requests.get(self.osm_url, stream=True) as r:
            with open(file_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)

        return file_path

    def osm_to_csv(self, osm_file_path: str) -> str:
        output_file = self._unique_file_name(ext="csv")
        try:
            cmd = [
                "/usr/bin/osmium",
                "export",
                "-f text",
                "-c config.json",
                "-O",
                f"-o {output_file}",
                osm_file_path,
            ]

            subprocess.check_output(" ".join(cmd), stderr=subprocess.STDOUT, shell=True)
            return output_file
        except subprocess.CalledProcessError as err:
            raise ConversionException(err.stdout)

    def _clean_geometry(
        self, wkt: str, geod: Geod, failFast: bool = False
    ) -> Optional[str]:  # noqa: C901
        if "POLYGON" not in wkt:
            return wkt

        geom = shp_wkt.loads(
            shp_wkt.dumps(shp_wkt.loads(wkt), rounding_precision=self.POLYGON_PRECISION)
        ).simplify(0)
        if geom.is_valid is False:
            if failFast is True:
                # TODO: print(f"INVALID [{explain_validity(geom)}] - {geom}")
                return None
            # Try validating one more time after attempting to clean with buffer
            return self._clean_geometry(shp_wkt.dumps(geom.buffer(0)), geod, True)
        elif geom.is_empty is True:
            # TODO: print(f"EMPTY - {geom}")
            return None

        area = abs(geod.geometry_area_perimeter(geom)[0])
        if area < self.MIN_GEOM_AREA:
            # TODO: print(f"AREA[{area}] - {geom}")
            return None

        return shp_wkt.dumps(geom, rounding_precision=5)

    def split_csv_file(
        self, csv_file: str, files: Dict[str, TextIO], attributes_tags: List[List[str]]
    ):
        geod = Geod(ellps="WGS84")
        with open(csv_file, "r") as fr:
            for row in fr:
                idx = row.rindex(" ")

                attribute_tag = self._get_row_attribute_tag(
                    # fmt: off
                    row[idx + 1: -1], attributes_tags
                    # fmt: on
                )
                if attribute_tag is None:
                    # TODO: Log out
                    continue

                wkt = self._clean_geometry(row[0:idx], geod)
                if wkt is None:
                    # TODO: Log out
                    continue

                file_key = f"{attribute_tag[0]}={attribute_tag[1]}"
                if file_key not in files:
                    continue

                files[file_key].write(f'"{wkt}","1"\n')

    def import_csv_to_cloud_storage(
        self, attributes_tags: List[List[str]], directory: str
    ):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_creds_path

        file_paths = [
            str(Path(directory, f"{attribute}_{tag}.csv"))
            for attribute, tag in attributes_tags
        ]

        num_threads = multiprocessing.cpu_count() * 2
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for url in file_paths:
                futures.append(executor.submit(self._upload_to_cloudstorage, url))

            for future in as_completed(futures):
                future.result()

    def calc(self):
        if self.csv_file is None:
            self.osm_file = self.osm_file or self.download_osm()
            self.csv_file = self.osm_to_csv(self.osm_file)

        attributes_tags = self._get_tags()
        if Path(self.directory).exists() is False:
            os.makedirs(self.directory)

        files = self._create_files(self.directory, attributes_tags)
        try:
            self.split_csv_file(self.csv_file, files, attributes_tags)
        finally:
            self._close_files(files)

        return self.import_csv_to_cloud_storage(attributes_tags, self.directory)

    def clean_up(self, **kwargs):
        if self.status == self.FAILED:
            return

        self._unlink(self.osm_file)
        self._unlink(self.csv_file)
        self._unlink(self.directory)


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
        help="Set a different source url to download OSM pbf file",
    )

    parser.add_argument(
        "-c",
        "--csv_file",
        type=str,
        help="CSV file to upload to Earth Engine.  Format: WKT,tag,burn",
    )

    options = parser.parse_args()
    task = HIIOSMCSV(**vars(options))
    task.run()
