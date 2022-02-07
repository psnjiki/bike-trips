## <span style="color:blue">**BIKE TRIPS**</span>
---
Collect historical trip data from major bike sharing companies:
- if needed stations and trips data are merged 
- upcoming and past holidays are added as well
- datetime columns (start and end of trip ) are broken down into elementary components: 
    i.e. year, month, hour, sec...


### <span style="color:blue">**Usage**</span>
---
#### <span style="color:blue">*- Command Line*</span>
To collect the files use the command:
```
python main.py --config=path/to/config.json --bike-sys=bike_system
```
*--bike-sys* expects a tag for the bike sharing company. The complete list is available in BIKESYS.md. Please visit the companies' websites to review license agreements on how to use the data.

*--config* expects a path to a json file. The content of the file should look like this:
```
{"years": "2020", "data_dir": "./data", "chunk_size": 400000}
```

- *years* is a query specifying which years should be downloaded. 
    (ex. "2020", "2020, 2021", "2019-2021", "2017, 2019-2021")
- *data_dir* is the directory where to store the data. defaults to "./"
- *chunksize* allows to control the memory by processing the data by chunks.
- all these arguments are optional. Do not include an argument in config if you don't need it.

#### <span style="color:blue">*- Writing a json file*</span>
```python
import json
args = {"years": "2020", "data_dir": "./data", "chunk_size": 400000}

with open('path/to/config.json', 'w') as f:
    json.dump(args, f)
```
#### <span style="color:blue">*- Passing urls to trip files*</span>
If you know the urls to trip files, you may process the data directly this way:
```python
from biketrips.bikesystem import selector
args = {"data_dir": "./data", "bike_sys": "bixi"}
trip_url = 'https://sitewebbixi.s3.amazonaws.com/uploads/docs/biximontreal-rentals-2021-07-805a45.zip'
selector(args).run(url_list=[trip_url])
```

#### <span style="color:blue">*- Output*</span>
The processed files (*trip_{}.csv*) are saved under a subdirectory named after bike_sys argument.

They are classified in folders according to the data source.

When collecting recent month data from current year, Just pass the year as input.

This will not reprocess previously downloaded months.

