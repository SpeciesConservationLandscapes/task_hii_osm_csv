import argparse
import os
import shutil
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from google.cloud.storage import Client  # type: ignore
from pyproj import Geod
from shapely import wkt as shp_wkt  # type: ignore
from task_base import HIITask  # type: ignore

from timer import timing


class ConversionException(Exception):
    pass


class HIIOSMCSV(HIITask):
    """

    Process:

    1. Fetch OSM pbf file
    2. Convert PBF file -> CSV files (for each layer) using OGR
    3. Upload filtered CSV file to Google Cloud Storage
    4. Using earthengine CLI load CSV as table (temporary) in EE

    """

    ee_osm_root = "osm"
    google_creds_path = "/.google_creds"

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

    def _unique_file_name(self, ext: str, prefix: Optional[str] = None) -> str:
        name = f"{uuid.uuid4()}.{ext}"
        if prefix:
            name = f"{prefix}-{name}"

        return name

    def _get_asset_id(self, attribute, tag, task_date):
        root = f"projects/{self.ee_project}/{self.ee_osm_root}"
        return f"{root}/{attribute}/{tag}/{tag}_{task_date}"

    def _upload_to_cloudstorage(self, src_path: str) -> str:
        targ_path = Path(src_path).name
        client = Client()
        bucket = client.bucket(os.environ["HII_OSM_BUCKET"])
        blob = bucket.blob(targ_path)
        blob.upload_from_filename(src_path)

        return targ_path

    @timing
    def download_osm(self) -> str:
        file_path = self._unique_file_name(ext="pbf")

        with requests.get(self.osm_url, stream=True) as r:
            with open(file_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)

        return file_path

    @timing
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

    @timing
    def add_burn_value(self, csv_file: str, output_file: str):
        geod = Geod(ellps="WGS84")
        with open(csv_file, "r") as fr:
            with open(output_file, "w") as fw:
                fw.write('"WKT","tag","burn"\n')
                for row in fr:
                    idx = row.rindex(" ")
                    wkt = row[0:idx]
                    tag = row[idx + 1 : -1]
                    if "POLYGON" in wkt:
                        geom = shp_wkt.loads(wkt)
                        area = abs(geod.geometry_area_perimeter(geom)[0])
                        if area < 5:  # 5m
                            continue

                    fw.write(f'"{wkt}","{tag}","1"\n')

    @timing
    def import_csv_to_cloud_storage(self, local_path: str) -> str:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_creds_path
        return self._upload_to_cloudstorage(local_path)

    def calc(self):
        if self.csv_file is None:
            self.osm_file = self.osm_file or self.download_osm()
            _csv_file = self.osm_to_csv(self.osm_file)
            self.csv_file = self.add_burn_value(_csv_file, f"{self.taskdate}.csv")

        return self.import_csv_to_cloud_storage(self.csv_file)

    def clean_up(self, **kwargs):
        if self.status == self.FAILED:
            return

        Path(self.osm_file).unlink(missing_ok=True)
        Path(self.csv_file).unlink(missing_ok=True)


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
