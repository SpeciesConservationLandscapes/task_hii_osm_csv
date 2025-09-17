from task import HIIOSMRasterize
htrt = HIIOSMRasterize(**{
    # 'osm_file': '/app/data/guatemala-250902.osm.pbf',
    'taskdate': '2012-12-31',
    'extent':"-91.5,15.5,-91.3,15.7"})
print(htrt)
htrt.run()