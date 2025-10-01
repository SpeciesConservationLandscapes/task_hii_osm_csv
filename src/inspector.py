# allows to run HIIOSMRasterize task in a debugger for future maintenance needs
# if you know you know
from task import HIIOSMRasterize
htrt = HIIOSMRasterize(**{
    'osm_url': 'https://download.geofabrik.de/central-america/belize-latest.osm.pbf',
    # 'osm_file': '/app/data/belize-latest.osm.pbf',
    'taskdate': '2021-12-31',
    'extent':"-88.8,16.95,-88.4,17.35",
    'backup_step_data':True,
    'cleanup':True
    }
)
print(htrt)
htrt.run()