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


# %%
# Cleaning up the house
miles_to_meters = 1609.34
feet_to_meters = 0.3048
secs_to_mins = 60
secs_to_hrs = 3600

base = duckdb.sql("SELECT * FROM data")

staging = duckdb.sql(f"""
    SELECT
        -- GRAIN
        id
        
        -- DIMENSIONS
        ,name
        ,type
        ,sport_type
        ,workout_type
        ,device_name
        ,start_date AS start_date_utc
        ,start_date_local
        ,timezone
        ,achievement_count
        ,kudos_count
        ,comment_count
        ,athlete_count
        ,photo_count
        ,map
        ,manual
        ,gear_id
        ,start_latlng
        ,end_latlng
        ,CASE
            WHEN LENGTH(CAST(month(start_date_local) AS VARCHAR))=1 THEN concat(year(start_date_local), '-0', month(start_date_local))
            ELSE concat(year(start_date_local), '-',  CAST(month(start_date_local) AS VARCHAR))
            END AS start_date_local_yyyy_mm
    
        -- STANDARDIZED MEASURES
        ,distance AS distance_meters
        ,moving_time AS moving_time_secs
        ,elapsed_time AS elapsed_time_secs
        ,elev_high AS elev_high_meters
        ,elev_low AS elev_low_meters
        ,total_elevation_gain AS total_elevation_gain_meters
        ,round(distance / {miles_to_meters},2) AS distance_miles
        ,round(moving_time / {secs_to_mins},2) AS moving_time_mins
        ,round(moving_time / {secs_to_hrs},2) AS moving_time_hrs
        ,round(elapsed_time / {secs_to_mins},2) AS elapsed_time_mins
        ,round(elapsed_time / {secs_to_hrs},2) AS elapsed_time_hrs
        ,round(total_elevation_gain / {feet_to_meters},2) AS total_elevation_gain_feet
        ,round(elev_high / {feet_to_meters},2) AS elevn_high_feet
        ,round(elev_low / {feet_to_meters},2) AS elev_low_feet

        -- OTHER MEASURES
        ,average_speed
        ,max_speed
        ,average_cadence
        ,average_watts
        ,max_watts
        ,weighted_average_watts
        ,device_watts
        ,kilojoules
        ,has_heartrate
        ,average_heartrate
        ,max_heartrate
        ,heartrate_opt_out
        ,display_hide_heartrate_option
        ,pr_count
        ,total_photo_count
        ,has_kudoed
        
        -- OTHER COLS
        ,upload_id
        ,upload_id_str
        ,external_id
        ,from_accepted_tag
        ,loaded_date
        
    FROM base
""")


#%%
#############################################
# 2025 Year In Sport
#############################################
runs_in_2025 = duckdb.sql('''
            SELECT *  
            FROM staging
            WHERE year(start_date_local)=2025
            AND type='Run'
           '''
   )

#%%
# How many miles did I run?
duckdb.sql('''
           WITH monthly AS (
                SELECT start_date_local_yyyy_mm,round(sum(distance_miles),2) as total_miles
                FROM runs_in_2025
                GROUP BY start_date_local_yyyy_mm
                ORDER BY start_date_local_yyyy_mm
            )
            , total AS (
                SELECT 'Grand total',round(sum(distance_miles),2) AS total_miles
                FROM runs_in_2025
                WHERE year(start_date_local)=2025
                AND type='Run'
            )
            SELECT * FROM monthly
            UNION ALL
            SELECT * FROM total
           '''
   )

#%%
# How many miles of each activity type did I do?
duckdb.sql('''
            SELECT type,round(sum(distance_miles),2) AS total_miles
            FROM staging
            WHERE year(start_date_local)=2025
            GROUP BY type
            HAVING total_miles>0
            ORDER BY total_miles DESC
           '''
   )


# %%
# How many runs did I do?
duckdb.sql('''
            SELECT count(*) AS total_runs
            FROM runs_in_2025
            WHERE year(start_date_local)=2025
            AND type='Run'
           '''
   )

#%%
# How many of each activity type did I do?
duckdb.sql('''
            SELECT type,count(*) AS activity_count
            FROM staging
            WHERE year(start_date_local)=2025
            GROUP BY type
            ORDER BY activity_count DESC
           '''
   )

#%%
# 3. How much elevation did I run?


#%%
# 4. How much total time did I run?


#%%
# 5. How much did these metrics change YoY?
