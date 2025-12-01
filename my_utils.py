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
def return_dir(dir_appendix: str) -> str:
    base_dir = os.path.dirname(__file__)
    output_dir = os.path.join(base_dir, dir_appendix)
    return output_dir
# %%
