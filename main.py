"""
main module. runs the job.

main.py --config=config.json --bike-sys=bixi

see BIKESYS.md for the list of available tags with the corresponding bike system tags. 

"""
import os
import json
import logging
import argparse
from biketrips.bikesystem import selector

if __name__ == "__main__":
    log_fmt = '%(asctime)s %(levelname)s %(name)s %(funcName)s : %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt, datefmt='%y/%m/%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description='bike-trips')
    parser.add_argument('--config', type=str, help='path string to config file')
    parser.add_argument('--bike-sys', type=str, help='bike system tag')
    parsed_args = parser.parse_args().__dict__

    with open(parsed_args['config'], 'r') as f:
        args = json.load(f)

    args['bike_sys'] = parsed_args['bike_sys']
    if 'data_dir' not in args:
        args['data_dir'] = '.'
    args['data_dir'] = os.path.join(args['data_dir'], args['bike_sys'])

    #for key in args:
    #    logger.info(f'args --> {key}: {args[key]} - type: {type(args[key])}')

    logger.info('data will be downloaded to {}'.format(os.path.abspath(args['data_dir'])))
    selector(args).run()
