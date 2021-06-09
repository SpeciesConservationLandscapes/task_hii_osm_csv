HII OSM CSV
-----------

## What does this task do?

1. Fetch OSM pbf file
2. Convert PBF file -> Text file (filter by attribute/tag list)
3.
  a) Split up text file into one CSV file per attribute/tag combination per 1 million rows
  b) Clean geometry and write out road tags to a roads CSV file
4. Rasterize each CSV file
5. Merge all tiff images into 1 multiband tiff file and split image
6. Upload to Google Storage
7. Clean up working directories and files


## Environment Variables

```
SERVICE_ACCOUNT_KEY=<GOOGLE SERVICE ACCOUNT KEY>
HII_OSM_BUCKET=hii-osm
OSM_DATA_SOURCE=https://osm.openarchive.site/planet-latest.osm.pbf
```

## Usage

*All parameters may be specified in the environment as well as the command line.*

```
/app # python task.py --help
usage: task.py [-h] [-d TASKDATE] [--overwrite] [-f OSM_FILE] [-u OSM_URL]
               [--osmium_text_file OSMIUM_TEXT_FILE] [-w WORKING_DIR]
               [--extent EXTENT] [--backup_step_data]
               [--osmium_config OSMIUM_CONFIG] [--no_roads]

optional arguments:
  -h, --help            show this help message and exit
  -d TASKDATE, --taskdate TASKDATE
  --overwrite           overwrite existing outputs instead of incrementing
                        (default: False)
  -f OSM_FILE, --osm_file OSM_FILE
                        Add local path to OSM source file. If not provided,
                        file will be downloaded (default: None)
  -u OSM_URL, --osm_url OSM_URL
                        Set a different source url to download OSM pbf file.
                        (default: None)
  --osmium_text_file OSMIUM_TEXT_FILE
                        Text file created from osmium export. (default: None)
  -w WORKING_DIR, --working_dir WORKING_DIR
                        Working directory to store files and directories
                        during processing. (default: None)
  --extent EXTENT       Output geographic bounds. (default: None)
  --backup_step_data    Backup up osm to text file to Google Cloud Storage
                        (default: False)
  --osmium_config OSMIUM_CONFIG
                        osmium config file (default: None)
  --no_roads            save out separate roads csv (default: False)
```
