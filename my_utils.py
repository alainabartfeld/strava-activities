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