import pandas as pd
import holidays
import requests
from zipfile import ZipFile
from src.utils import get_calendar_holidays, walk_dir
import os

import logging
logger = logging.getLogger(__name__)


class Trip():
    def __init__(self, data_dir):
        self.data_dir = data_dir
    
    def download(self, url):
        """
        get bixi trip data from internet. 
        return the folder where data is saved.
        """
        file_name = url.split('/')[-1]
        save_dir = os.path.join(self.data_dir, file_name.split('.')[0])
        file_path = os.path.join(save_dir, file_name)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir) 
            with open(file_path, 'wb') as f:
                f.write(requests.get(url).content)
            if '.zip' in file_name:
                with ZipFile(file_path, 'r') as z:
                    z.extractall(path=save_dir)
                os.remove(file_path)
            else:
                with open(file_path, 'wb') as f:
                    data = requests.get(url).content
                    f.write(data)
        else:
            print('directory already exist')
            return None
        return save_dir
    
    @staticmethod
    def break_datetime(df, column):
        """
        given a datetime string column of a dataframe, create new columns
        representing different dims of datetime (year, month, day, hour ...)
        """
        name = column.replace('date', '')
        s = pd.to_datetime(df[column]).copy()
        df[name + 'dt'] = s.dt.date
        df[name + 'year'] = s.dt.year
        df[name + 'month'] = s.dt.month
        df[name + 'day'] = s.dt.day
        df[name + 'hour'] = s.dt.hour
        df[name + 'minute'] = s.dt.minute
        df[name + 'second'] = s.dt.second
        df[name + 'time_ratio'] = (s.dt.hour + s.dt.minute/60 + s.dt.second/3600)/24
        df[name + 'day_of_week'] = s.map(lambda dt: dt.isoweekday())
        df.drop(column, axis=1, inplace=True)
        return df

    def run_bixi(self, url):
        #download/unzip data from the web
        save_dir = self.download(url)
        if save_dir is None:
            return None # process only new files
        
        #load stations and trips data with pandas
        files = walk_dir(save_dir)
        logger.info(f'loading:\n {url}')
        logger.info(f'found files: {files}')

        station_files = [file for file in files if file.lower().split('/')[-1].find('stations') >= 0]
        trip_files = [file for file in files if file.lower().split('/')[-1].find('od_') >= 0]
        logger.info(f'stations files: {station_files}')
        logger.info(f'trip files: {trip_files}')
        
        stations_df = pd.concat(
            [pd.read_csv(file) for file in station_files],
            axis=0,
            ignore_index=True).drop_duplicates()
        
        trip_df = pd.concat(
            [pd.read_csv(file) for file in trip_files],
            axis=0,
            ignore_index=True).drop_duplicates()
        
        # standardize column names
        rename_dict = {
            'emplacement_pk_start': 'start_station_code',
            'emplacement_pk_end': 'end_station_code',
            'pk': 'code'
        }
        stations_df.rename(rename_dict, axis=1, inplace=True)
        trip_df.rename(rename_dict, axis=1, inplace=True)
        
        logger.info('stations_df shape {}'.format(stations_df.shape))
        logger.info('trip_df shape {}'.format(trip_df.shape))
        
        # merge stations and trips
        df = trip_df.merge(
            stations_df, 
            how='left',
            left_on='start_station_code',
            right_on='code').drop('code', axis=1)
        
        df = df.merge(
            stations_df, 
            how='left',
            left_on='end_station_code',
            right_on='code', 
            suffixes=('', '_end')).drop('code', axis=1) #.drop('emplacement_pk_end', axis=1)
                
        # add datetime elements
        self.break_datetime(df, column='start_date')
        self.break_datetime(df, column='end_date')
        
        # add holidays
        calendar = get_calendar_holidays(
            dt_series=df['start_dt'].unique(), 
            holidays=holidays.CA(prov='QC'))

        df = df.merge(calendar, left_on='start_dt', right_on='dt', how='left').drop('dt', axis=1)
        
        logger.info('df shape {}'.format(df.shape))
        # write processed df to file
        df.to_csv(os.path.join(save_dir, 'trip.csv'), index=False)
        
        logger.info('saving df to {}'.format(os.path.join(save_dir, 'trip.csv')))
    
    def run(self, url, src):
        if src == 'bixi':
            self.run_bixi(url)
