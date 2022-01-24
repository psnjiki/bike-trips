
## BIKE TRIPS 

This package collects and preprocess historical trip data from major bike sharing companies in Canada (currently available for bixi-montreal only):
- if needed stations and trips data are merged 
- upcoming and past holidays features are added as well
- datetime columns (start and end of trip ) are broken down into elementary components: i.e. year, month, hour, sec...


To process files use the command:
```
python main.py --config="./config.json"
```

--config expects a path to a json files. The content should look like this:
```
{
    "source": "bixi",
    "search_url": "https://bixi.com/en/open-data",
    "data_dir": "./data",
    "search_dict": {"class":"document-csv col-md-2 col-sm-4 col-xs-12"} 
}
```

source argument is a tag for the bike system.
search_url is the url to webpage containing the public dataset.
data_dir is the directory where to store the data.
use search_dict argument to set additional criteria to identify which documents to download from webpage.

ex: get all bixi's historical data
```python
"search_dict": {"class":"document-csv col-md-2 col-sm-4 col-xs-12"}
```

ex: get all bixi's data from year 2016
    
```python
"search_dict": {"text":"2016", "class":"document-csv col-md-2 col-sm-4 col-xs-12"}
```
Easily configure yoour own config file:

```python
args = {
    "search_url": "https://bixi.com/en/open-data",
    "source": "bixi",
    "search_dict": {"text":2016, "class":"document-csv col-md-2 col-sm-4 col-xs-12"},
    "data_dir": "./data"
}

import json
with open('path/to/config.json', 'w') as f:
    json.dump(args, f)
```

Also if you know the url to the trip file, process it this way:

```python
from src.trip import Trip

trip_url = 'https://sitewebbixi.s3.amazonaws.com/uploads/docs/biximontreal-rentals-2021-07-805a45.zip'
data_dir = '.'

Trip(data_dir).run(trip_url, src='bixi')

```
