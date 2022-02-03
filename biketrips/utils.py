"""
Various utility functions used by main modules.
"""
import os
import re
from datetime import timedelta
from datetime import datetime

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup


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
    for delta in range(366):
        if direction == 'next':
            day = holidays.get(date + timedelta(days=delta))
        elif direction == 'previous':
            day = holidays.get(date + timedelta(days=-delta))
        if day:
            break
    return delta, day

def get_calendar_holidays(dt_series, holidays):
    """
    given an input date series and holidays object,
    return a dataframe with next and previous holidays type and delay to/from
    """
    calendar = pd.DataFrame(dt_series, columns=['dt']).sort_values('dt')
    def func(direction):
        def myfunc(date):
            return dist_to_holiday(
                date,
                holidays=holidays,
                direction=direction)
        return myfunc
    next_h = np.array(list(calendar['dt'].map(func('next'))))
    calendar['next_delay'] = next_h[:, 0]
    calendar['next_holiday'] = next_h[:, 1]
    prev_h = np.array(list(calendar['dt'].map(func('previous'))))
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

def html_doc(url, search_cfg):
    """
    extract all html elements from web page satisfying a search criteria
    """
    data = requests.get(url).text
    docs = BeautifulSoup(data, "lxml").find_all(**search_cfg)
    return docs

def href_from_doc_list(doc_list, tag='href'):
    """
    extract a given tag from list of html elements.
    """
    return [doc.get(tag) for doc in doc_list]

def text_from_doc_list(doc_list):
    """
    extract a text from list of html elements.
    """
    return [doc.text for doc in doc_list]

def href_from_url(url, search_cfg):
    """
    extract hyperlinks from web page.
    requires a search_cfg to determine which links to download.
    """
    data = requests.get(url).text
    docs = BeautifulSoup(data, "lxml").find_all(**search_cfg)
    return [doc.get('href') for doc in docs]

def years_query(query):
    """
    process a year query and returns the list of years integers
    satisfying the query.
    ex.
    "2020,2021,2023" --> [2020, 2021, 2023]
    "2020-2022" --> [2020, 2021, 2022]
    "2020, 2021-2023" --> [2020, 2021, 2022, 2023]
    "2020-" --> [2020, 2021, ..., datetime.now().year]
    """
    coma_sep = [comp.strip() for comp in query.split(",")]
    years_list = []
    for comp in coma_sep:
        if comp.find('-') >= 0:
            start, end = comp.split('-')
            if len(start.strip()) == 0:
                start = 2010
            else:
                start = int(start.strip())
            if len(end.strip()) == 0:
                end = datetime.now().year
            else:
                end = int(end.strip())
            years_list += list(range(start, end + 1))

        else:
            if len(comp.strip()) > 0:
                years_list.append(int(comp))
    return list(set(years_list))

def is_in_path(file_path, find_str):
    """
    check if find_tr is in filename given file path
    """
    file_path = file_path.lower()
    find_str = find_str.lower()
    return file_path.split('/')[-1].find(find_str) >= 0
