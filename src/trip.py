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
    def load_bixi(files, chunksize):
        station_files = [file for file in files if file.lower().split('/')[-1].find('stations') >= 0]
        trip_files = [file for file in files if file.lower().split('/')[-1].find('od_') >= 0]
        
        trip_dfs = [pd.read_csv(file, chunksize=chunksize) for file in trip_files]
        
        stations_df = pd.concat(
            [pd.read_csv(file) for file in station_files],
            axis=0,
            ignore_index=True).drop_duplicates()
        
        logger.info(f'stations files: {station_files}')
        logger.info(f'trip files: {trip_files}')
        return trip_dfs, stations_df
    
    @staticmethod
    def break_datetime(df, columns):
        """
        given a datetime string column of a dataframe, create new columns
        representing various dims of datetime (year, month, day, hour ...)
        """
        for column in columns:
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
    
    def station_trip_join(self, stations_df, trip_df, rename_dict):
        """
        
        """
        # standardize column names        
        stations_df.rename(rename_dict, axis=1, inplace=True)
        trip_df.rename(rename_dict, axis=1, inplace=True)
                
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
            suffixes=('', '_end')).drop('code', axis=1)
        return df
    
    def process(self, stations_df, df, rename_dict,
                save_dir, save_name):
        """
        merge stations and trip df. add datetime component. add time to next and previous holiday.
        write down the result.
        """
        # merge stations ad trips
        if stations_df is not None:
            df = self.station_trip_join(stations_df, df, rename_dict)
                
        # add datetime elements
        self.break_datetime(df, columns=['start_date', 'end_date'])
        
        # add holidays
        calendar = get_calendar_holidays(
            dt_series=df['start_dt'].unique(), 
            holidays=holidays.CA(prov='QC'))

        df = df.merge(calendar, left_on='start_dt', right_on='dt', how='left').drop('dt', axis=1)
        
        # write processed df to file
        df.to_csv(os.path.join(save_dir, save_name), index=False)
        logger.info('{}: df shape {}'.format(save_name, df.shape))

    def run_bixi(self, url, chunksize=None):
        """
        """
        #download/unzip data from the web
        logger.info(f'downloading:\n {url}')
        save_dir = self.download(url)
        if save_dir is None:
            return None # process only new files
        
        #load stations and trips data with pandas
        files = walk_dir(save_dir)        
        trip_dfs, stations_df = self.load_bixi(files, chunksize)
        
        rename_dict = {
            'emplacement_pk_start': 'start_station_code',
            'emplacement_pk_end': 'end_station_code',
            'pk': 'code'
        }
        
        #TODO: parallelize this loop
        if chunksize:
            for i, ds in enumerate(trip_dfs):
                with ds:
                    j = 0
                    for chunk in ds:
                        self.process(
                            stations_df=stations_df,
                            df=chunk, 
                            rename_dict=rename_dict,
                            save_dir=save_dir, 
                            save_name=f'trip_{i}_{j}.csv')
                        j += 1
        else:
            for i, ds in enumerate(trip_dfs):
                self.process(
                    stations_df=stations_df,
                    df=ds, 
                    rename_dict=rename_dict,
                    save_dir=save_dir, 
                    save_name=f'trip_{i}.csv')
    
    
    def run(self, url, src, chunksize):
        if src == 'bixi':
            self.run_bixi(url, chunksize=chunksize)
