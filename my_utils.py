#%%
import glob
import duckdb
import os
import logging
from datetime import datetime
import requests
import pandas as pd

#%%
def get_today_as_date():
    today = datetime.today().strftime("%Y-%m-%d")
    return(today)


def get_today_as_timestamp():
    timestamp_today = pd.Timestamp.today()
    return(timestamp_today)


def setup_logging(log_file_path):
    logging.basicConfig(
        filename=log_file_path,
        filemode="a",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    

def get_specific_path(sub_path: str):
    # __file__ is the path to this utils file, not where it's called from
    specific_path = os.path.join(os.path.dirname(__file__), sub_path)
    return(specific_path)


def refresh_access_token(auth_url: str= "https://www.strava.com/oauth/token"):
    logging.info("Starting the refresh_access_token() function")
    
    CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
    CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
    REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")

    if not CLIENT_ID or not CLIENT_SECRET or not REFRESH_TOKEN:
        logging.error("Missing one or more required environment variables.")
        raise EnvironmentError("Set STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, STRAVA_REFRESH_TOKEN")
    
    auth_params = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "grant_type": "refresh_token"
    }

    auth_response = requests.post(auth_url, data=auth_params)

    if auth_response.status_code != 200:
        logging.error(f"Token refresh failed: {auth_response.text}")
        raise Exception("Failed to refresh access token.")

    access_token = auth_response.json().get("access_token")
    logging.info("Access token retrieved successfully.")

    return(access_token)


def initialize_paths(log_dir: str, data_dir: str, filename: str):
    today = get_today_as_date()

    # Set up paths and file names (paths must already exist)
    # Paths
    log_dir = get_specific_path(log_dir)
    data_dir = get_specific_path(data_dir)
    # File names
    filename = f"{filename}_{today}"
    log_file_path = os.path.join(log_dir, filename+".log")
    csv_file_path = os.path.join(data_dir, filename+".csv")

    return({"log_file_path":log_file_path
            , "csv_file_path":csv_file_path})


#%%
def download_data_from_strava(csv_path: str, activities_url: str = "https://www.strava.com/api/v3/athlete/activities"):
    logging.info("Starting the download_data_from_strava() function")

    # Refresh access token
    access_token = refresh_access_token()
    
    # ---------------------------------------------------------
    # Get Activities
    # ---------------------------------------------------------
    logging.info("Starting request for activities from Strava API")
    headers = {"Authorization": f"Bearer {access_token}"}

    # Full load of all activities ever
    all_activities = []
    page = 1

    while True:
        logging.info("Requesting page %d of activities", page)
        params = {"page": page, "per_page": 200}  # 200 is max allowed
        response = requests.get(activities_url, headers=headers, params=params)
        if response.status_code != 200:
            logging.error(f"Activities API request failed: {response.text}")
            raise Exception("Failed to fetch activities.")
        batch = response.json()

        if not batch:
            break

        all_activities.extend(batch)
        page += 1

    logging.info(f"Fetched {len(all_activities)} activities.")

    # ---------------------------------------------------------
    # Save Activities to CSV
    # ---------------------------------------------------------
    # convert to data frame
    df = pd.DataFrame(all_activities)
    # add loaded date
    df["loaded_date"] = get_today_as_timestamp()
    # convert to csv
    if not csv_path:
        logging.error("No csv_path provided to download_data_from_strava")
        raise ValueError("csv_path is required")
    df.to_csv(csv_path, index=False)

    logging.info(f"download_data_from_strava() function completed")

    return(df)

#%%
def upload_data_to_duckdb(df: pd.DataFrame):
    logging.info(f"Starting upload_data_to_duckdb() function")
    
    # Get all the files in the "data" directory
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data'))
    csv_files = sorted(glob.glob(os.path.join(data_dir, '*.csv')))
    logging.info(f"Found CSV files, getting the latest file from data directory: {data_dir}")

    # Get the latest file based on date and version
    file_info = []  # list of tuples: (date, version, filepath)

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
        
    ddb = duckdb.read_csv(latest_file)
    logging.info(f"Reading the latest CSV file read into DuckDB: {latest_file}")
    logging.info(f"upload_data_to_duckdb() function completed")
    
    return(ddb)
# %%
