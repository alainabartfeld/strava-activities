#%%
import my_utils
import duckdb
import logging
from jinja2 import Template

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
runs_in_2025 = duckdb.sql('''
            SELECT *  
            FROM staging
            WHERE year(start_date_local) = 2025
            AND type = 'Run'
           '''
   )

#%%
# How many miles did I run in 2025?
# Per month and grand total
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
            )
            SELECT * FROM monthly
            UNION ALL
            SELECT * FROM total
           '''
   )

#%%
# How many miles of each activity type did I do in 2025?
duckdb.sql('''
            SELECT type, round(sum(distance_miles),2) AS total_miles
            FROM staging
            WHERE year(start_date_local) = 2025
            GROUP BY type
            HAVING total_miles > 0
            ORDER BY total_miles DESC
           '''
   )


# %%
# How many runs did I do in 2025?
duckdb.sql('''
            SELECT count(*) AS total_runs
            FROM runs_in_2025
           '''
   )

#%%
# How many of each activity type did I do in 2025?
duckdb.sql('''
            SELECT type,count(*) AS activity_count
            FROM staging
            WHERE year(start_date_local)=2025
            GROUP BY type
            ORDER BY activity_count DESC
           '''
   )

#%%
# How much elevation did I run in 2025?
duckdb.sql('''
            SELECT round(sum(total_elevation_gain_feet),2) AS total_elevation_gain_feet
            FROM runs_in_2025
           '''
   )

#%%
# How much total time did I run in hours in 2025?
duckdb.sql('''
            SELECT round(sum(moving_time_hrs),2) AS total_moving_time_hrs
            FROM runs_in_2025
           '''
   )

#%%
# How did moving time differ from activity time in 2025?
duckdb.sql('''
            SELECT round(sum(moving_time_hrs),2) AS total_moving_time_hrs
                ,round(sum(elapsed_time_hrs),2) AS total_elapsed_time_hrs
                ,concat(round((total_moving_time_hrs/total_elapsed_time_hrs)*100,2),'%') as pct_moving_time
            FROM runs_in_2025
           '''
   )

#%%
# How much did these metrics change YoY?
# 2023-2025 metrics
basic_table = duckdb.sql('''
        SELECT 
            year(start_date_local) AS year
            -- Total distance
            ,round(sum(distance_miles),2) AS total_miles
            -- Number of runs
            ,count(*) AS total_runs
            -- Total elevation gain
            ,round(sum(total_elevation_gain_feet),2) AS total_elevation_gain_feet
            -- Total moving time
            ,round(sum(moving_time_hrs),2) AS total_moving_time_hrs
        FROM staging
        WHERE year > 2022
            AND type = 'Run'
        GROUP BY year
        ORDER BY year
        '''
    )

# YoY change
measures = [
    'distance_miles'
    ,'total_elevation_gain_feet'
    ,'moving_time_hrs'
]

changes_sql_template = Template('''
        WITH changes AS (
            SELECT     
                YEAR(start_date_local) AS year
                
                -- Number of runs
                ,COUNT(*) AS number_of_runs
                
                -- Number of runs percentage change
                ,ROUND(
                    (COUNT(*) 
                        - LAG(COUNT(*)) OVER (ORDER BY year(start_date_local))
                    )
                    / LAG(COUNT(*)) OVER (ORDER BY year(start_date_local))
                    * 100
                , 2) AS pct_change_number_of_runs

                {% for col in measures %}
                
                    -- Total {{ col }}
                    ,ROUND(SUM({{ col }}), 2) AS {{ col if col.startswith('total_') else 'total_' ~ col }}

                    -- {{ col }} percentage change
                    ,ROUND(
                        (
                            ROUND(SUM({{ col }}), 2)
                            - LAG(ROUND(SUM({{ col }}), 2))
                                OVER (ORDER BY year(start_date_local))
                        )
                        / LAG(ROUND(SUM({{ col }}), 2))
                            OVER (ORDER BY year(start_date_local))
                        * 100
                    , 2) AS pct_change_{{ col }}

                {% endfor %}
                
            FROM staging
            WHERE year > 2022
                AND type = 'Run'
            GROUP BY year
            ORDER BY year
        )
        SELECT *
        FROM changes
        '''
    )
 
yoy = duckdb.sql(changes_sql_template.render(measures=measures))

# Format the percentage changes
pct_change_measures = [
    'pct_change_distance_miles'
    ,'pct_change_number_of_runs'
    ,'pct_change_total_elevation_gain_feet'
    ,'pct_change_moving_time_hrs'
]

format_yoy_sql_template = Template('''
                WITH formats AS (
                    SELECT
                        *
                        {% for col in metrics %}
                        ,CASE
                            WHEN {{ col }} IS NULL THEN 'N/A'
                            ELSE CONCAT(CAST( {{ col }} AS VARCHAR), '%')
                        END AS {{ col }}_formatted
                        {% endfor %}
                    FROM yoy
                )
                SELECT 
                    {% for col in metrics %}
                        {{ col }}_formatted AS {{ col }}{% if not loop.last %},{% endif %}
                    {% endfor %}
                FROM formats                                
        '''
)

duckdb.sql(format_yoy_sql_template.render(metrics=pct_change_measures))

