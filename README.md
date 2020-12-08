HII OSM CSV
-----------

## What does this task do?

1. Fetch OSM pbf file
2. Convert PBF file -> Text file (filter by attribute/tag list)
3. Split up text file by attribute and tags into CSV files
    -> cleans and validates geometry
4. Using CSV files to Google Storage



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
