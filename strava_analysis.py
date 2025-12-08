import my_utils
import duckdb

data = my_utils.upload_data_to_duckdb(my_utils.download_data_from_strava())
duckdb.sql("DESCRIBE data")