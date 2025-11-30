#%%
import os
import requests
import pandas as pd
import logging
from datetime import datetime

#%%
# ---------------------------------------------------------
# Create Log File Name (Daily + Incrementing)
# ---------------------------------------------------------
today = datetime.today().strftime("%Y-%m-%d")
base_log_name = f"strava_export_{today}.log"
log_dir = os.path.dirname(__file__)

# Find next available log filename if duplicates exist
log_file_path = os.path.join(log_dir, base_log_name)
counter = 1

while os.path.exists(log_file_path):
    log_file_path = os.path.join(log_dir, f"strava_export_{today}_{counter}.log")
    counter += 1

# ---------------------------------------------------------
# Setup Logging
# ---------------------------------------------------------
logging.basicConfig(
    filename=log_file_path,
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Strava export script started.")

#%%
# ---------------------------------------------------------
# Environment Variables
# ---------------------------------------------------------
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
auth_url = "https://www.strava.com/oauth/token"
auth_params = {
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "refresh_token": REFRESH_TOKEN,
    "grant_type": "refresh_token"
}

logging.info("Requesting new access token...")
auth_response = requests.post(auth_url, data=auth_params)

if auth_response.status_code != 200:
    logging.error(f"Token refresh failed: {auth_response.text}")
    raise Exception("Failed to refresh access token.")

access_token = auth_response.json().get("access_token")
logging.info("Access token retrieved successfully.")

# ---------------------------------------------------------
# Step 2: Get Activities
# ---------------------------------------------------------
activities_url = "https://www.strava.com/api/v3/athlete/activities"
headers = {"Authorization": f"Bearer {access_token}"}

logging.info("Requesting activities from Strava API...")
activities_response = requests.get(activities_url, headers=headers)

if activities_response.status_code != 200:
    logging.error(f"Activities API request failed: {activities_response.text}")
    raise Exception("Failed to fetch activities.")

activities = activities_response.json()
logging.info(f"Fetched {len(activities)} activities.")

# ---------------------------------------------------------
# Step 3: Save Activities to CSV With Today's Date + Incrementing
# ---------------------------------------------------------
base_csv_name = f"strava_export_{today}.csv"
csv_path = os.path.join(log_dir, base_csv_name)

# Increment filename if multiple exports on the same day
csv_counter = 1
while os.path.exists(csv_path):
    csv_path = os.path.join(log_dir, f"strava_export_{today}_{csv_counter}.csv")
    csv_counter += 1

df = pd.DataFrame(activities)
df.to_csv(csv_path, index=False)

logging.info(f"CSV exported: {os.path.basename(csv_path)}")
print(f"Export complete → {os.path.basename(csv_path)}")
print(f"Log file → {os.path.basename(log_file_path)}")

# %%
