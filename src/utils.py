
import pandas as pd
import numpy as np

from datetime import timedelta

from bs4 import BeautifulSoup
import requests

import os
import re


def walk_dir(directory):
    """
    list recursively all files in directory
    """
    files_list = []
    for path, _, files in os.walk(directory):
        for file in files:
            files_list.append(os.path.join(path, file))
    return files_list

def dist_to_holiday(date, holidays, direction='next'):
    """
    find the next holiday and current delta between date and holiday.
    Look for previous holiday if direction is 'previous'.
    """
    for n in range(366):
        if direction == 'next':
            day = holidays.get(date + timedelta(days=n))
        elif direction == 'previous':
            day = holidays.get(date + timedelta(days=-n))   
        if day:
            return n, day

def get_calendar_holidays(dt_series, holidays):
    """
    given an input date series and holidays object,
    return a dataframe with next and previous holidays type and delay to/from
    """
    calendar = pd.DataFrame(dt_series, columns=['dt']).sort_values('dt')
    
    next_h = np.array(list(calendar['dt'].map(lambda dt: dist_to_holiday(dt, holidays=holidays, direction='next'))))
    calendar['next_delay'] = next_h[:, 0]
    calendar['next_holiday'] = next_h[:, 1]

    prev_h = np.array(list(calendar['dt'].map(lambda dt: dist_to_holiday(dt, holidays=holidays, direction='previous'))))
    calendar['prev_delay'] = prev_h[:, 0]
    calendar['prev_holiday'] = prev_h[:, 1]
    return calendar

def search_config(**kwargs):
    """wrapper for html search criteria"""
    config = {}
    if 'text' in kwargs:
        config['text'] = re.compile(str(kwargs.pop('text')))
    if 'name' in kwargs:
        config['name'] = kwargs.pop('name')
    config['attrs'] = kwargs    
    return config

def href_from_url(url, search_config):
    """
    extract hyperlinks from web page.
    requires a search_config to determine which links to download.
    """
    data = requests.get(url).text
    docs = BeautifulSoup(data, "lxml").find_all(**search_config)
    return [doc.get('href') for doc in docs]
