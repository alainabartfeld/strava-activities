# One-time script for getting refresh tokens
#%%
import os
import requests

#%%
CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
CODE = os.getenv("STRAVA_CODE")

resp = requests.post(
    "https://www.strava.com/api/v3/oauth/token",
    data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": CODE,
        "grant_type": "authorization_code"
    }
)

print(resp.json())

# %%
