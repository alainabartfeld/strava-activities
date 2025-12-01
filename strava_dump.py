#%%
import os
import requests
import pandas as pd
import logging
from datetime import datetime

#%%
today = datetime.today().strftime("%Y-%m-%d")
log_counter = 1
csv_counter = 1

# Setup Logging
# Paths and file names (paths must already exist)
BASE_DIR = os.path.dirname(__file__)
LOG_DIR = os.path.join(BASE_DIR, "logs")
DATA_DIR = os.path.join(BASE_DIR, "data")

base_log_name = f"strava_export_{today}.log"
log_file_path = os.path.join(LOG_DIR, base_log_name)

base_csv_name = f"strava_export_{today}.csv"
csv_path = os.path.join(DATA_DIR, base_csv_name)

while os.path.exists(log_file_path):
    log_file_path = os.path.join(LOG_DIR, f"strava_export_{today}_{log_counter}.log")
    log_counter += 1


logging.basicConfig(
    filename=log_file_path,
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info(f"Data deposited into: {csv_path}")
logging.info(f"Logs deposited into: {log_file_path}")

# Environment variables
CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")

if not CLIENT_ID or not CLIENT_SECRET or not REFRESH_TOKEN:
    logging.error("Missing one or more required environment variables.")
    raise EnvironmentError("Set STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, STRAVA_REFRESH_TOKEN")


#%%
# ---------------------------------------------------------
# Step 1: Refresh Token → Get Access Token
# ---------------------------------------------------------
logging.info("Strava export script started.")
logging.info("Requesting new access token")

auth_url = "https://www.strava.com/oauth/token"
auth_params = {
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "refresh_token": REFRESH_TOKEN,
    "grant_type": "refresh_token"
}
activities_url = "https://www.strava.com/api/v3/athlete/activities"

auth_response = requests.post(auth_url, data=auth_params)

if auth_response.status_code != 200:
    logging.error(f"Token refresh failed: {auth_response.text}")
    raise Exception("Failed to refresh access token.")

access_token = auth_response.json().get("access_token")
logging.info("Access token retrieved successfully.")

#%%
# ---------------------------------------------------------
# Step 2: Get Activities
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


#%%
# ---------------------------------------------------------
# Step 3: Save Activities to CSV
# ---------------------------------------------------------
while os.path.exists(csv_path):
    csv_path = os.path.join(DATA_DIR, f"strava_export_{today}_{csv_counter}.csv")
    csv_counter += 1

df = pd.DataFrame(all_activities)
df.to_csv(csv_path, index=False)

logging.info(f"Script completed.")
