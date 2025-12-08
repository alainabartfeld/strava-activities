#%%
import duckdb
import glob
import json
import logging
from datetime import datetime
import os
import my_utils

# TODO
# load somehwere query-able
# get full refresh each day per the latest strava_export file

#%%
today = my_utils.get_today_as_date()

# Set up logging
BASE_DIR = os.path.dirname(__file__)
LOG_DIR = os.path.join(BASE_DIR, "logs")

base_log_name = f"duckdb{today}.log"
log_file_path = os.path.join(LOG_DIR, base_log_name)


while os.path.exists(log_file_path):
    log_file_path = os.path.join(LOG_DIR, f"duckdb{today}_{log_counter}.log")
    log_counter += 1


logging.basicConfig(
    filename=log_file_path,
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info(f"Log deposited into: {log_file_path}")

#%%
# TODO make this a function
# Extract dates from filenames and find the max to get the latest data
    
data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data'))
csv_files = sorted(glob.glob(os.path.join(data_dir, '*.csv')))
logging.info(f"Found CSV files: {csv_files}")

file_info = []  # list of tuples: (date, version, filepath)
logging.info("Getting the latest file from data directory")

for f in csv_files:
    filename = os.path.basename(f)    
    # Remove prefix
    fn = filename.replace("strava_export_", "")  # "YYYY-MM-DD.csv" or "YYYY-MM-DD_N.csv"
    fn = fn.replace(".csv", "")
    parts = fn.split("_")
    # First part is always the date
    date_str = parts[0]
    
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        print("date: ",date)
    except ValueError:
        logging.error(f"Skipping malformed filename: {filename}")
        continue  # skip malformed files
    # Version handling
    if len(parts) > 1 and parts[1].isdigit():
        version = int(parts[1])
    else:
        version = 1  # default for files with no suffix

    file_info.append((date, version, f))

# If no valid files found
if not file_info:
    logging.error("No valid CSV files found")
    latest_file = None
else:
    # Find latest date overall
    latest_date = max(x[0] for x in file_info)
    # Get only files for that date
    latest_date_files = [x for x in file_info if x[0] == latest_date]
    # Among files for that date, choose one with highest version
    latest_file_entry = max(latest_date_files, key=lambda x: x[1])
    latest_file = latest_file_entry[2]
    
    logging.info("\nLatest file:", os.path.basename(latest_file))
    logging.info("Date:", latest_date.strftime("%Y-%m-%d"))
    logging.info("Version:", latest_file_entry[1])


#%%
# Connect to DuckDB to query the data
ddb = duckdb.read_csv(latest_file)
