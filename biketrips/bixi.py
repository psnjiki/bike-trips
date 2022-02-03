"""
A module containing tools to download and process trips historical data
from bikesharing system Bixi
"""
import logging
import pandas as pd
import holidays as hld

from biketrips.utils import years_query
from biketrips.utils import is_in_path
from biketrips.utils import href_from_url
from biketrips.utils import search_config
from biketrips.loader import Trip


SEARCH_URL = 'https://bixi.com/en/open-data'
SEARCH_DICT = {"class": "document-csv col-md-2 col-sm-4 col-xs-12"}
COUNTRY = 'CA'
PROV = 'QC'
STATE = None
RENAME_DICT = {
    'emplacement_pk_start': 'start_station_code',
    'emplacement_pk_end': 'end_station_code',
    'pk': 'code'}

logger = logging.getLogger(__name__)

class Bixi(Trip):
    """
    A class to download and process trips historical data
    from bikesharing system Bixi
    """
    def __init__(self, args):
        super(Bixi, self).__init__(args['data_dir'])
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
        station_files = [file for file in files if is_in_path(file, 'stations')]
        trip_files = [file for file in files if is_in_path(file, 'od_')]

        trip_dfs = [pd.read_csv(file, chunksize=chunksize) for file in trip_files]

        stations_df = pd.concat(
            [pd.read_csv(file) for file in station_files],
            axis=0,
            ignore_index=True).drop_duplicates()

        logger.info('stations files: {}'.format(station_files))
        logger.info('trip files: {}'.format(trip_files))
        return trip_dfs, stations_df

    def station_trip_join(self, stations_df, trip_df):
        """
        merge trip data and stations
        """
        # merge stations and trips
        merged_df = trip_df.merge(
            stations_df,
            how='left',
            left_on='start_station_code',
            right_on='code').drop('code', axis=1)

        merged_df = merged_df.merge(
            stations_df,
            how='left',
            left_on='end_station_code',
            right_on='code',
            suffixes=('', '_end')).drop('code', axis=1)
        return merged_df

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
            url_list = href_from_url(SEARCH_URL, search_cfg)
            url_list = self.href_filter(url_list, self.years_list)
            
        logger.info('url_list: {}'.format(url_list))
        for url in url_list:
            self.run_url(
                url=url,
                rename_dict=RENAME_DICT,
                holidays=hdays,
                chunksize=self.chunksize)
