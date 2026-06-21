#%%
import my_strava_utils
import logging
from jinja2 import Template

#%%
# Set up logging
init_paths = my_strava_utils.initialize_paths("logs", "strava_data", "strava_export")

my_strava_utils.setup_logging(init_paths["log_file_path"])
logging.info("Starting Strava Analysis Pipeline")
logging.info(f"Log deposited into: {init_paths['log_file_path']}")
logging.info(f"Data deposited into: {init_paths['csv_file_path']}")

#%%
# Get the data
data = my_strava_utils.upload_data_to_duckdb(my_strava_utils.download_data_from_strava(init_paths['csv_file_path']))
logging.info("Strava Analysis Pipeline completed")