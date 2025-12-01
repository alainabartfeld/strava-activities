#%%
import duckdb
import glob
import os
import json
import logging
from datetime import datetime
import requests
import pandas as pd

#%%
def get_base_path():
    BASE_DIR = os.path.dirname(__file__)
    return BASE_DIR

def get_today_as_date():
    today = datetime.today().strftime("%Y-%m-%d")
    return(today)

def get_today_as_timestamp():
    timestamp_today = pd.Timestamp.today()
    return(timestamp_today)