HII OSM CSV
-----------

## What does this task do?

1. Fetch OSM pbf file
2. Convert PBF file -> CSV files (for each layer) using OGR
3. Upload filtered CSV file to Google Cloud Storage
4. Using earthengine CLI load CSV as table (temporary) in EE



## Environment Variables

```
SERVICE_ACCOUNT_KEY=<GOOGLE SERVICE ACCOUNT KEY>
HII_OSM_BUCKET=hii-osm
OSM_DATA_SOURCE=https://osm.openarchive.site/planet-latest.osm.pbf
```

## Usage

```
/app # python task.py --help
usage: task.py [-h] [-d TASKDATE] [-f OSM_FILE] [-u OSM_URL] [-c CSV_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -d TASKDATE, --taskdate TASKDATE
  -f OSM_FILE, --osm_file OSM_FILE
                        Add local path to OSM source file. If not provided, file will be downloaded
  -u OSM_URL, --osm_url OSM_URL
                        Set a different source url to download OSM pbf file
  -c CSV_FILE, --csv_file CSV_FILE
                        CSV file to upload to Earth Engine. Format: WKT,tag,burn
```


## Limitation

For performance reasons, it is assumed that the OSM tags are unique between attributes.