
## BIKE TRIPS 

Collect historical trip data from major bike sharing companies:
- if needed stations and trips data are merged 
- upcoming and past holidays features are added as well
- datetime columns (start and end of trip ) are broken down into elementary components: 
    i.e. year, month, hour, sec...

To collect the files use the command:
```
python main.py --config=path/to/config.json --bike-sys=bike_system
```

--config expects a path to a json file. The content  of the json file should look like this:
```
{"years": "2020", "data_dir": "./data", "chunk_size": 400000}

```

- *years* is a query specifying which years should be downloaded. 
    (ex. "2020", "2020, 2021", "2019-2021", "2017, 2019-2021")
- *data_dir* is the directory where to store the data. defaults to "./"
- *chunksize* (optional) allows to control the memory by processing the data by chunks.
- all these arguments are optional. Do not include an argument in config if you don't need it.
ex: get all bixi's historical data

This downloads only data that does not already exists in the directory. 

Easily configure your own config file:

```python
args = {"years": "2020", "data_dir": "./data", "chunk_size": 400000}

import json
with open('path/to/config.json', 'w') as f:
    json.dump(args, f)
```

If you have the url to trip files, you may process it this way:

```python
from biketrips.bikesystem import selector
args = {"data_dir": "./data", "bike_sys": "bixi"}
trip_url = 'https://sitewebbixi.s3.amazonaws.com/uploads/docs/biximontreal-rentals-2021-07-805a45.zip'
selector(args).run(url_list=[trip_url])
```
