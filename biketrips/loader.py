"""
Meta tools processing data
Class: Trip
"""
import os
from abc import ABC, abstractmethod
import logging
from zipfile import ZipFile

import pandas as pd
import requests

from biketrips.utils import get_calendar_holidays
from biketrips.utils import walk_dir


logger = logging.getLogger(__name__)

class Trip(ABC):
    """
    Base class for processing data
    """
    def __init__(self, data_dir):
        self.data_dir = data_dir

    @staticmethod
    def href_filter(url_list, years_list):
        """
        filter elements of url_list containing any year string in years_list
        """
        res = []
        for url in url_list:
            for year in years_list:
                if url.find(str(year)) >= 0:
                    res.append(url)
        return res

    @staticmethod
    def break_datetime(data, columns):
        """
        given a datetime string column of a dataframe, create new columns
        representing various dims of datetime (year, month, day, hour ...)
        """
        for column in columns:
            name = column.replace('date', '')
            series = pd.to_datetime(data[column]).copy()
            data[name + 'dt'] = series.dt.date
            data[name + 'year'] = series.dt.year
            data[name + 'month'] = series.dt.month
            data[name + 'day'] = series.dt.day
            data[name + 'hour'] = series.dt.hour
            data[name + 'minute'] = series.dt.minute
            data[name + 'second'] = series.dt.second
            time_ratio = (series.dt.hour + series.dt.minute/60 + series.dt.second/3600)/24
            data[name + 'time_ratio'] = time_ratio
            data[name + 'day_of_week'] = series.map(lambda dt: dt.isoweekday())
            data.drop(column, axis=1, inplace=True)
        return data

    def download(self, url):
        """
        get bixi trip data from internet.
        return the folder where data is saved.
        """
        #download/unzip data from the web
        logger.info(f'downloading:\n {url}')
        file_name = url.split('/')[-1]
        save_dir = os.path.join(self.data_dir, file_name.split('.')[0])
        file_path = os.path.join(save_dir, file_name)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
            with open(file_path, 'wb') as file:
                file.write(requests.get(url).content)
            if file_name.split('.')[-1] == 'zip':
                with ZipFile(file_path, 'r') as zip_file:
                    zip_file.extractall(path=save_dir)
                os.remove(file_path)
            elif file_name.split('.')[-1] == 'csv':
                with open(file_path, 'wb') as file:
                    data = requests.get(url).content
                    file.write(data)
            else:
                logger.info('skip unsupported extension: {}'.format(file_name.split('.')[-1]))
                return None
        else:
            print('directory already exist')
            return None

        files = walk_dir(save_dir)
        return save_dir, files

    def process(self, stations_df, trip_df, rename_dict, save_dir, save_name, holidays):
        """
        merge stations and trip df. add datetime component.
        add time to next and previous holiday.
        write down the result.
        """
        # standardize column names
        trip_df.rename(rename_dict, axis=1, inplace=True)

        # merge stations ad trips
        if stations_df is not None:
            stations_df.rename(rename_dict, axis=1, inplace=True)
            trip_df = self.station_trip_join(stations_df, trip_df)

        # add datetime elements
        self.break_datetime(trip_df, columns=['start_date', 'end_date'])

        # add holidays
        calendar = get_calendar_holidays(
            dt_series=trip_df['start_dt'].unique(),
            holidays=holidays)

        trip_df = trip_df.merge(calendar, left_on='start_dt', right_on='dt', how='left')
        trip_df.drop('dt', axis=1, inplace=True)

        # write processed df to file
        trip_df.to_csv(os.path.join(save_dir, save_name), index=False)
        logger.info('{}: trip_df shape {}'.format(save_name, trip_df.shape))

    @staticmethod
    @abstractmethod
    def load(files, chunksize):
        """
        download files from url then load as pandas df
        """

    @abstractmethod
    def station_trip_join(self, stations_df, trip_df):
        """
        merge trip data and stations
        """

    def run_url(self, url, rename_dict, holidays, chunksize):
        """
        process data from one url source.
        """
        #download/unzip data from the web
        downloads = self.download(url)

        if downloads is not None: # process only new files
            save_dir, files = downloads

            #load stations and trips data with pandas
            trip_dfs, stations_df = self.load(files, chunksize)

            #TODO: parallelize this loop
            if chunksize:
                for i, chung_gen in enumerate(trip_dfs):
                    with chung_gen:
                        j = 0
                        for chunk in chung_gen:
                            self.process(
                                stations_df=stations_df,
                                trip_df=chunk,
                                rename_dict=rename_dict,
                                save_dir=save_dir,
                                save_name=f'trip_{i}_{j}.csv',
                                holidays=holidays)
                            j += 1
            else:
                for i, data in enumerate(trip_dfs):
                    self.process(
                        stations_df=stations_df,
                        trip_df=data,
                        rename_dict=rename_dict,
                        save_dir=save_dir,
                        save_name=f'trip_{i}.csv',
                        holidays=holidays)
