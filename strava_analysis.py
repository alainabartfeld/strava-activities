#%%
import my_utils
import duckdb
import logging

#%%
# Set up logging
init_paths = my_utils.initialize_paths("logs", "data", "strava_export")

my_utils.setup_logging(init_paths["log_file_path"])
logging.info("Starting Strava Analysis Pipeline")
logging.info(f"Log deposited into: {init_paths['log_file_path']}")
logging.info(f"Data deposited into: {init_paths['csv_file_path']}")

#%%
data = my_utils.upload_data_to_duckdb(my_utils.download_data_from_strava(init_paths['csv_file_path']))
logging.info("Strava Analysis Pipeline completed")

#%%
duckdb.sql("DESCRIBE data")
duckdb.sql("SELECT * FROM data")