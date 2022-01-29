
from src.utils import href_from_url, search_config
from src.trip import Trip

import json

import logging
import argparse


if __name__ == "__main__":
    log_fmt = '%(asctime)s %(levelname)s %(name)s %(funcName)s : %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt, datefmt='%y/%m/%d %H:%M:%S')
    logger = logging.getLogger(__name__)
    
    parser = argparse.ArgumentParser(description='my-trips')
    parser.add_argument('--config', type=str, help='path string to config file')
    parsed_args = parser.parse_args().__dict__
  
    for key in parsed_args:
         logger.info(f'parsed {key}: {parsed_args[key]}')
    
    with open(parsed_args['config'], 'r') as f:
        args = json.load(f)

    for key in args:
         logger.info(f'parsed {key}: {args[key]}')
    
    url_list = href_from_url(args['search_url'], search_config(**args['search_dict']))
    
    logger.info(f'url_list: {url_list}')
    
    mytrip = Trip(args['data_dir'])
    
    if 'chunksize' in args:
        chunksize = args['chunksize']
    else:
        chunksize = None

    for url in url_list:
        mytrip.run(url, src=args['source'], chunksize=chunksize)
    
