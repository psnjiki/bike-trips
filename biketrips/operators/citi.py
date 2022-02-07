"""
A module containing tools to download and process trips data
from citi bike New York
"""
import logging
import pandas as pd
import holidays as hld

from biketrips.utils import years_query
from biketrips.utils import search_config
from biketrips.loader import Trip
from biketrips.loader import RENAME_DICT


SEARCH_URL = 'https://s3.amazonaws.com/tripdata'
SEARCH_DICT = {"name": "key"}
COUNTRY = 'US'
PROV = None
STATE = 'NY'

logger = logging.getLogger(__name__)


class Citi(Trip):
    """
    A class to download and process trips historical data
    from bikesharing system Bixi
    """
    def __init__(self, args):
        super(Citi, self).__init__(args['data_dir'])
        self.rename_dict = RENAME_DICT
        if 'chunk_size' in args:
            self.chunksize = args['chunk_size']
        else:
            self.chunksize = None
        if 'years' in args:
            self.years_list = years_query(args['years'])
        else:
            self.years_list = years_query('-')

    @staticmethod
    def load(files, chunksize):
        """
        download files from url then load as pandas df
        """
        trip_files = [file for file in files if file.find('__MACOSX/') < 0]
        logger.info('trip files: {}'.format(trip_files))
        trip_dfs = [pd.read_csv(file, chunksize=chunksize) for file in trip_files]
        #logger.info('trip files: {}'.format(trip_files))
        return trip_dfs, None

    def run(self, url_list=None):
        """
        collect and save data
        a url_list containing the links of files 
        to download can be passed directly.
        if not provided it eill be infered class arguments (config file)
        """
        hdays = hld.CountryHoliday(
            country=COUNTRY,
            prov=PROV,
            state=STATE)

        search_cfg = search_config(**SEARCH_DICT)

        if url_list is None:
            url_list = self.get_url_list(
                url=SEARCH_URL,
                search_cfg=search_cfg,
                attr='text',
                tag=None,
                prefix=SEARCH_URL)
            url_list = self.href_filter(url_list, self.years_list)

        logger.info('url_list: {}'.format(url_list))
        for url in url_list:
            self.run_url(
                url=url,
                rename_dict=self.rename_dict,
                holidays=hdays,
                chunksize=self.chunksize)
