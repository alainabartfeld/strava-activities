"""
Download all activity data from COROS Training Hub.

Uses the COROS API to authenticate and fetch activity data including:
- Activity metadata (name, type, distance, duration, timestamps)
- FIT files for detailed sensor data
- GPX files for GPS tracks

Usage:
    Set environment variables COROS_EMAIL and COROS_PASSWORD, then run:
    python get_coros_data.py

    Or run interactively and enter credentials when prompted.
"""

import os
import json
import logging
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, asdict
from enum import Enum

import requests
import bcrypt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

COROS_BASE_URL = "https://teamapi.coros.com"
AUTH_ENDPOINT = f"{COROS_BASE_URL}/account/login"
ACTIVITIES_ENDPOINT = f"{COROS_BASE_URL}/activity/query"
ACTIVITY_DETAIL_ENDPOINT = f"{COROS_BASE_URL}/activity/detail/query"
DOWNLOAD_ENDPOINT = f"{COROS_BASE_URL}/activity/detail/download"

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://training.coros.com",
    "Referer": "https://training.coros.com/",
    "Content-Type": "application/json",
}

SPORT_TYPE_MAP = {
    100: "running",
    101: "trail_running",
    102: "track_run",
    103: "indoor_run",
    104: "hiking",
    200: "cycling",
    201: "mountain_biking",
    202: "indoor_cycling",
    203: "gravel_biking",
    300: "swimming",
    301: "pool_swim",
    302: "open_water_swim",
    400: "gym_cardio",
    401: "rowing",
    402: "strength",
    500: "triathlon",
    600: "skiing",
    601: "snowboarding",
    602: "xc_skiing",
    700: "multisport",
    800: "walk",
    900: "other",
    901: "indoor_walk",
    902: "jump_rope",
    903: "climbing",
    904: "yoga",
    905: "pilates",
}

FILE_TYPE_MAP = {
    "csv": 0,
    "gpx": 1,
    "kml": 2,
    "tcx": 3,
    "fit": 4,
}


class CorosAuthError(Exception):
    pass


class CorosAPIError(Exception):
    pass


@dataclass
class ActivitySummary:
    activity_id: str
    activity_name: str
    activity_type: str
    sport_type_code: int
    start_time: datetime
    end_time: datetime
    workout_seconds: int
    total_seconds: int
    distance_meters: float
    calories: Optional[int] = None
    avg_heart_rate: Optional[int] = None
    avg_pace: Optional[float] = None
    adjusted_pace: Optional[float] = None
    avg_speed: Optional[float] = None
    best_km_pace: Optional[float] = None
    total_ascent: Optional[float] = None
    total_descent: Optional[float] = None
    avg_cadence: Optional[int] = None
    step_count: Optional[int] = None
    avg_power: Optional[int] = None
    training_load: Optional[int] = None
    device: Optional[str] = None
    device_id: Optional[str] = None
    image_url: Optional[str] = None

    def to_dict(self):
        d = asdict(self)
        d["start_time"] = self.start_time.isoformat()
        d["end_time"] = self.end_time.isoformat()
        return d


def compute_coros_bcrypt(password: str) -> tuple[bytes, bytes]:
    password_md5_hex = hashlib.md5(password.encode("utf-8")).hexdigest()
    salt = bcrypt.gensalt(rounds=10)
    hashed = bcrypt.hashpw(password_md5_hex.encode("utf-8"), salt)
    return hashed, salt


class CorosClient:
    def __init__(self, email: str, password: str, timeout: int = 30):
        self.email = email
        self.password = password
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(BROWSER_HEADERS)
        self.accesstoken: Optional[str] = None
        self.user_id: Optional[str] = None

    def _check_response(self, data: dict) -> None:
        if data.get("result") != "0000":
            error_message = data.get("message", "Unknown error")
            result_code = data.get("result", "unknown")
            if result_code == "1019":
                raise CorosAuthError(f"Access token invalid: {error_message}")
            raise CorosAPIError(f"API error (code {result_code}): {error_message}")

    def authenticate(self) -> bool:
        logger.info(f"Authenticating with COROS as {self.email}")
        hashed, salt = compute_coros_bcrypt(self.password)

        payload = {
            "account": self.email,
            "accountType": 2,
            "p1": hashed.decode("utf-8"),
            "p2": salt.decode("utf-8"),
        }

        try:
            response = self.session.post(AUTH_ENDPOINT, json=payload, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            if data.get("result") != "0000":
                raise CorosAuthError(f"Auth failed: {data.get('message', 'Unknown error')}")

            self.accesstoken = data["data"]["accessToken"]
            self.user_id = str(data["data"]["userId"])
            self.session.cookies["CPL-coros-token"] = self.accesstoken

            logger.info(f"Authentication successful (userId: {self.user_id})")
            logger.debug(f"Access token: {self.accesstoken[:20]}...")
            return True

        except requests.RequestException as e:
            raise CorosAuthError(f"Network error: {e}") from e

    def _get_auth_headers(self) -> dict:
        return {
            "accesstoken": self.accesstoken,
            "yfheader": f'{{"userId":"{self.user_id}"}}'
        }

    def get_activities(self, limit_per_page: int = 200) -> list[ActivitySummary]:
        if not self.accesstoken:
            raise CorosAPIError("Not authenticated. Call authenticate() first.")

        all_activities = []
        page = 1

        while True:
            params = {"size": min(limit_per_page, 200), "pageNumber": page, "modeList": ""}
            response = self.session.get(
                ACTIVITIES_ENDPOINT,
                params=params,
                headers=self._get_auth_headers()
            )
            response.raise_for_status()

            data = response.json()
            self._check_response(data)

            page_data = data.get("data", {})
            activities_raw = page_data.get("dataList", [])
            total_pages = page_data.get("totalPage", 1)

            for act in activities_raw:
                try:
                    sport_code = act.get("sportType", 900)
                    summary = ActivitySummary(
                        activity_id=str(act["labelId"]),
                        activity_name=act.get("name", "Unnamed"),
                        activity_type=SPORT_TYPE_MAP.get(sport_code, "other"),
                        sport_type_code=sport_code,
                        start_time=datetime.fromtimestamp(act["startTime"]),
                        end_time=datetime.fromtimestamp(act["endTime"]),
                        workout_seconds=act.get("workoutTime", 0),
                        total_seconds=act.get("totalTime", 0),
                        distance_meters=float(act.get("distance", 0)),
                        calories=act.get("calorie"),
                        avg_heart_rate=act.get("avgHr"),
                        avg_pace=act.get("avgSpeed"),
                        adjusted_pace=act.get("adjustedPace"),
                        avg_speed=act.get("avgSpeed"),
                        best_km_pace=act.get("bestKm"),
                        total_ascent=act.get("ascent"),
                        total_descent=act.get("descent"),
                        avg_cadence=act.get("avgCadence"),
                        step_count=act.get("step"),
                        avg_power=act.get("avgPower"),
                        training_load=act.get("trainingLoad"),
                        device=act.get("device"),
                        device_id=act.get("deviceId"),
                        image_url=act.get("imageUrl"),
                    )
                    all_activities.append(summary)
                except Exception as e:
                    logger.warning(f"Failed to parse activity: {e}")

            logger.info(f"Fetched page {page}/{total_pages} ({len(activities_raw)} activities)")

            if page >= total_pages:
                break
            page += 1

        return all_activities

    def download_activity_file(
        self,
        activity_id: str,
        sport_type_code: int,
        file_format: str,
        output_path: Path
    ) -> bool:
        if not self.accesstoken:
            raise CorosAPIError("Not authenticated.")

        file_type = FILE_TYPE_MAP.get(file_format.lower())
        if file_type is None:
            raise ValueError(f"Unknown format: {file_format}. Use: {list(FILE_TYPE_MAP.keys())}")

        params = {
            "labelId": activity_id,
            "sportType": sport_type_code,
            "fileType": file_type
        }

        response = self.session.get(
            DOWNLOAD_ENDPOINT,
            params=params,
            headers=self._get_auth_headers()
        )
        response.raise_for_status()

        data = response.json()
        self._check_response(data)

        file_url = data.get("data", {}).get("fileUrl")
        if not file_url:
            logger.warning(f"No file URL returned for activity {activity_id}")
            return False

        file_response = self.session.get(file_url)
        file_response.raise_for_status()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(file_response.content)

        return True

    def get_activity_detail(self, activity_id: str, sport_type_code: int) -> Optional[dict]:
        """Fetch detailed activity data including laps, HR zones, gear, etc."""
        if not self.accesstoken:
            raise CorosAPIError("Not authenticated.")

        headers = self._get_auth_headers()
        headers["Content-Type"] = "application/x-www-form-urlencoded"

        form_data = {
            "labelId": activity_id,
            "userId": self.user_id,
            "sportType": str(sport_type_code)
        }

        response = self.session.post(
            ACTIVITY_DETAIL_ENDPOINT,
            data=form_data,
            headers=headers
        )
        response.raise_for_status()

        data = response.json()
        self._check_response(data)

        return data.get("data", {})


def main():
    email = os.getenv("COROS_EMAIL")
    password = os.getenv("COROS_PASSWORD")

    if not email:
        email = input("COROS email: ").strip()
    if not password:
        import getpass
        password = getpass.getpass("COROS password: ")

    output_dir = Path(__file__).parent / "coros_data"
    output_dir.mkdir(exist_ok=True)

    client = CorosClient(email, password)
    client.authenticate()

    logger.info("Fetching all activities...")
    activities = client.get_activities()
    logger.info(f"Found {len(activities)} total activities")

    metadata_file = output_dir / "activities_metadata.json"
    with open(metadata_file, "w") as f:
        json.dump([a.to_dict() for a in activities], f, indent=2)
    logger.info(f"Saved metadata to {metadata_file}")

    state_file = output_dir / ".download_state.json"
    downloaded_ids = set()
    if state_file.exists():
        with open(state_file) as f:
            downloaded_ids = set(json.load(f).get("downloaded_ids", []))

    NO_GPS_ACTIVITY_TYPES = {
        "strength", "yoga", "pilates", "indoor_run",
        "indoor_walk", "pool_swim", "rowing"
    }

    new_downloads = 0

    for i, activity in enumerate(activities, 1):
        if activity.activity_id in downloaded_ids:
            continue

        date_str = activity.start_time.strftime("%Y-%m-%d")
        base_name = f"{date_str}_{activity.activity_type}_{activity.activity_id}"

        formats_to_download = ["fit"]
        if activity.activity_type not in NO_GPS_ACTIVITY_TYPES:
            formats_to_download.append("gpx")

        fit_success = False
        for fmt in formats_to_download:
            output_path = output_dir / f"{base_name}.{fmt}"
            if output_path.exists():
                if fmt == "fit":
                    fit_success = True
                continue

            try:
                if client.download_activity_file(
                    activity.activity_id,
                    activity.sport_type_code,
                    fmt,
                    output_path
                ):
                    logger.info(f"[{i}/{len(activities)}] Downloaded {output_path.name}")
                    if fmt == "fit":
                        fit_success = True
                else:
                    logger.warning(f"[{i}/{len(activities)}] No {fmt} available for {activity.activity_name}")
            except CorosAPIError as e:
                if "1031" in str(e) and fmt == "gpx":
                    logger.debug(f"[{i}/{len(activities)}] No GPS data for {activity.activity_name}")
                else:
                    logger.error(f"[{i}/{len(activities)}] Failed to download {fmt} for {activity.activity_name}: {e}")
            except Exception as e:
                logger.error(f"[{i}/{len(activities)}] Failed to download {fmt} for {activity.activity_name}: {e}")

        if fit_success:
            downloaded_ids.add(activity.activity_id)
            new_downloads += 1

        with open(state_file, "w") as f:
            json.dump({"downloaded_ids": list(downloaded_ids)}, f)

    logger.info(f"Download complete. {new_downloads} new activities downloaded to {output_dir}")


if __name__ == "__main__":
    main()
