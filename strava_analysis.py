#%%
import my_utils
import duckdb
import logging
from jinja2 import Template

#%%
# Set up logging
init_paths = my_utils.initialize_paths("logs", "strava_data", "strava_export")

my_utils.setup_logging(init_paths["log_file_path"])
logging.info("Starting Strava Analysis Pipeline")
logging.info(f"Log deposited into: {init_paths['log_file_path']}")
logging.info(f"Data deposited into: {init_paths['csv_file_path']}")

#%%
# Get the data
data = my_utils.upload_data_to_duckdb(my_utils.download_data_from_strava(init_paths['csv_file_path']))
logging.info("Strava Analysis Pipeline completed")


# %%
# Clean up the house
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
        
        , CASE 
            WHEN distance_miles = 0
            THEN 0.00
            ELSE round(moving_time_mins / distance_miles,2) 
        END AS average_pace_mins_per_mile
        
        -- OTHER MEASURES
        ,average_speed --in meters per second by default
        ,max_speed --in meters per second by default
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
# Only runs in 2025
runs_in_2025 = duckdb.sql('''
            SELECT *  
            FROM staging
            WHERE year(start_date_local) = 2025
            AND type = 'Run'
           '''
   )

#%%
##########################################################################################
# 2025 YEAR IN SPORT
##########################################################################################

#%%
# Top sport
duckdb.sql('''
            SELECT type, COUNT(*) AS activity_frequency
            FROM staging
            WHERE year(start_date_local) = 2025
            GROUP BY type
            ORDER BY activity_frequency DESC
           '''
   )

#%%
# Total days active (not just running)
# Per month and grand total
duckdb.sql('''
           WITH monthly AS (
            SELECT distinct start_date_local_yyyy_mm, count(distinct(date(start_date_local))) AS total_days_active
            FROM staging
            WHERE year(start_date_local) = 2025
            GROUP BY ALL
            HAVING COUNT(*) >= 1
            ORDER BY start_date_local_yyyy_mm
           )
            , total AS (
                SELECT 'Grand total'
                    ,count(distinct(date(start_date_local))) AS total_days_active
                FROM staging
                WHERE year(start_date_local) = 2025
                GROUP BY ALL
                HAVING COUNT(*) >= 1
            )
            , pct AS (
                SELECT 'Percentage of days active'
                , CONCAT(CAST(ROUND((total_days_active/365)*100,2) AS VARCHAR),'%')
                FROM total
            )
            SELECT * FROM monthly
            UNION ALL
            SELECT * FROM total
            UNION ALL
            SELECT * FROM pct
           '''
   )

#%%
# Total days just running
# Per month and grand total
duckdb.sql('''
           WITH monthly AS (
            SELECT distinct start_date_local_yyyy_mm, count(distinct(date(start_date_local))) AS total_days_running
            FROM runs_in_2025
            GROUP BY ALL
            HAVING COUNT(*) >= 1
            ORDER BY start_date_local_yyyy_mm
           )
            , total AS (
                SELECT 'Grand total'
                    ,count(distinct(date(start_date_local))) AS total_days_running
                FROM runs_in_2025
                GROUP BY ALL
                HAVING COUNT(*) >= 1
            )
            , pct AS (
                SELECT 'Percentage of days active'
                , CONCAT(CAST(ROUND((total_days_running/365)*100,2) AS VARCHAR),'%')
                FROM total
            )
            SELECT * FROM monthly
            UNION ALL
            SELECT * FROM total
            UNION ALL
            SELECT * FROM pct
           '''
   )

#%%
# Total distance active (not just running)
# Per month and grand total
duckdb.sql('''
           WITH monthly AS (
                SELECT start_date_local_yyyy_mm,round(sum(distance_miles),2) as total_miles_active
                FROM staging
                WHERE year(start_date_local) = 2025
                GROUP BY start_date_local_yyyy_mm
                ORDER BY start_date_local_yyyy_mm
            )
            , total AS (
                SELECT 'Grand total',round(sum(distance_miles),2) AS total_miles_active
                FROM staging
                WHERE year(start_date_local) = 2025
            )
            , monthly_avg AS
                (
                SELECT 'Monthly average',round(sum(distance_miles)/12,2)
                FROM staging
                WHERE year(start_date_local) = 2025
                )
            SELECT * FROM monthly
            UNION ALL
            SELECT * FROM total
            UNION ALL
            SELECT *
            FROM monthly_avg
           '''
   )

#%%
# Total distance just running
# Per month and grand total
duckdb.sql('''
           WITH monthly AS (
                SELECT start_date_local_yyyy_mm,round(sum(distance_miles),2) as total_miles_running
                FROM runs_in_2025
                GROUP BY start_date_local_yyyy_mm
                ORDER BY start_date_local_yyyy_mm
            )
            , total AS (
                SELECT 'Grand total',round(sum(distance_miles),2) AS total_miles_running
                FROM runs_in_2025
            )
            , monthly_avg AS
                (
                SELECT 'Monthly average',round(sum(distance_miles)/12,2)
                FROM runs_in_2025
                )
            SELECT * FROM monthly
            UNION ALL
            SELECT * FROM total
            UNION ALL
            SELECT *
            FROM monthly_avg
           '''
   )

#%%
# Total time active (not just running)
# Per month and grand total
duckdb.sql('''
           WITH monthly AS (
            SELECT distinct start_date_local_yyyy_mm, round(sum(moving_time_hrs),2) as moving_time_hrs_active
            FROM staging
            WHERE year(start_date_local) = 2025
            GROUP BY ALL
            HAVING COUNT(*) >= 1
            ORDER BY start_date_local_yyyy_mm
           )
            , total AS (
                SELECT 'Grand total',round(sum(moving_time_hrs),2) as moving_time_hrs_active
                FROM staging
                WHERE year(start_date_local) = 2025
                GROUP BY ALL
                HAVING COUNT(*) >= 1
            )
            , monthly_avg AS
                (
                SELECT 'Monthly average',round(sum(moving_time_hrs)/12,2)
                FROM staging
                WHERE year(start_date_local) = 2025
                )
            SELECT * FROM monthly
            UNION ALL
            SELECT * FROM total
            UNION ALL
            SELECT *
            FROM monthly_avg
           '''
   )

#%%
# Total time just running
# Per month and grand total
duckdb.sql('''
           WITH monthly AS (
            SELECT distinct start_date_local_yyyy_mm, round(sum(moving_time_hrs),2) as moving_time_hrs_running
            FROM runs_in_2025
            GROUP BY ALL
            HAVING COUNT(*) >= 1
            ORDER BY start_date_local_yyyy_mm
           )
            , total AS (
                SELECT 'Grand total',round(sum(moving_time_hrs),2) as moving_time_hrs_running
                FROM runs_in_2025
                GROUP BY ALL
                HAVING COUNT(*) >= 1
            )
            , monthly_avg AS
                (
                SELECT 'Monthly average',round(sum(moving_time_hrs)/12,2)
                FROM runs_in_2025
                )
            SELECT * FROM monthly
            UNION ALL
            SELECT * FROM total
            UNION ALL
            SELECT *
            FROM monthly_avg
           '''
   )

#%%
# Total elevation active (not just running)
# Per month and grand total
duckdb.sql('''
           WITH monthly AS (
                SELECT start_date_local_yyyy_mm,round(sum(total_elevation_gain_feet),2) as elevation_gain_feet_active
                FROM staging
                WHERE year(start_date_local) = 2025
                GROUP BY start_date_local_yyyy_mm
                ORDER BY start_date_local_yyyy_mm
               )
            , total AS (
                SELECT 'Grand total',round(sum(total_elevation_gain_feet),2) AS elevation_gain_feet_active
                FROM staging
                WHERE year(start_date_local) = 2025
            )
            SELECT * FROM monthly
            UNION ALL
            SELECT * FROM total
           '''
   )

#%%
# Total elevation just running
# Per month and grand total
mt_everest_height = 29032
duckdb.sql(f'''
           WITH monthly AS (
                SELECT start_date_local_yyyy_mm,round(sum(total_elevation_gain_feet),2) AS elevation_gain_feet_running
                FROM runs_in_2025
                GROUP BY start_date_local_yyyy_mm
                ORDER BY start_date_local_yyyy_mm
               )
            , total AS (
                SELECT 'Grand total',round(sum(total_elevation_gain_feet),2) AS elevation_gain_feet_running
                FROM runs_in_2025
            )
            , everest AS (
                SELECT 'Number of times climbed Mt. Everest'
                    , ROUND(elevation_gain_feet_running / {mt_everest_height},2)
                FROM total
            )
            SELECT * FROM monthly
            UNION ALL
            SELECT * FROM total
            UNION ALL
            SELECT * FROM everest
           '''
   )

#%%
# Longest weekly activity streak (not just running)
# There are 52 weeks populated so I had at least one activity per week in 2025
duckdb.sql('''
        SELECT count(DISTINCT
            week(start_date_local)) AS activity_weeks
        FROM staging
        WHERE year(start_date_local) = 2025 
    '''
)

#%%
# Longest weekly running streak
# TODO: Convert the week numbers back into activity_dates to be more intuitive
duckdb.sql('''
    -- 1. One row per run week w/ assumption of if there is an entry in the Strava data, there was an activity logged
    WITH activity_weeks AS (
        SELECT DISTINCT
            week(start_date_local) AS activity_week
        FROM runs_in_2025
    ),

    -- 2. Order and look at previous day
    ordered_days AS (
        SELECT
            activity_week
            ,LAG(activity_week) OVER (ORDER BY activity_week) AS prev_date
        FROM activity_weeks
    ),

    -- 3. Flag when a new streak starts
    streak_flags AS (
        SELECT
            activity_week
            ,CASE
                WHEN prev_date IS NULL THEN 1
                WHEN activity_week = prev_date + 1 THEN 0
                ELSE 1
            END AS new_streak
        FROM ordered_days
    ),

    -- 4. Assign streak group id
    streak_groups AS (
        SELECT
            activity_week
            ,SUM(new_streak) OVER (ORDER BY activity_week) AS streak_id
        FROM streak_flags
    ),

    -- 5. Count weeks per streak
    streak_lengths AS (
        SELECT
            streak_id
            ,COUNT(*) AS streak_length
            ,MIN(activity_week) AS streak_start
            ,MAX(activity_week) AS streak_end
        FROM streak_groups
        GROUP BY streak_id
    )

    -- 6. Max streak and when
    SELECT
        MAX(streak_length) AS max_running_streak_week
        ,streak_start
        ,streak_end
    FROM streak_lengths
    WHERE streak_length = 
        (SELECT MAX(streak_length)
        FROM streak_lengths)
    GROUP BY streak_id,streak_start,streak_end
    '''
)


# %%
##########################################################################################
# ADDITIONAL QUESTIONS
##########################################################################################

#%%
# Longest daily streak of activity (not just run)
duckdb.sql('''
    -- 1. One row per active day w/ assumption of if there is an entry in the Strava data, there was an activity logged
    WITH activity_days AS (
        SELECT DISTINCT
            DATE(start_date_local) AS activity_date
        FROM staging
        WHERE year(start_date_local) = 2025
    ),

    -- 2. Order and look at previous day
    ordered_days AS (
        SELECT
            activity_date
            ,LAG(activity_date) OVER (ORDER BY activity_date) AS prev_date
        FROM activity_days
    ),

    -- 3. Flag when a new streak starts
    streak_flags AS (
        SELECT
            activity_date
            ,CASE
                WHEN prev_date IS NULL THEN 1
                WHEN activity_date = prev_date + INTERVAL 1 DAY THEN 0
                ELSE 1
            END AS new_streak
        FROM ordered_days
    ),

    -- 4. Assign streak group id
    streak_groups AS (
        SELECT
            activity_date
            ,SUM(new_streak) OVER (ORDER BY activity_date) AS streak_id
        FROM streak_flags
    ),

    -- 5. Count days per streak
    streak_lengths AS (
        SELECT
            streak_id
            ,COUNT(*) AS streak_length
            ,MIN(activity_date) AS streak_start
            ,MAX(activity_date) AS streak_end
        FROM streak_groups
        GROUP BY streak_id
    )

    -- 6. Max streak and when
    SELECT
        MAX(streak_length) AS max_activity_streak_days
        ,streak_start
        ,streak_end
    FROM streak_lengths
    WHERE streak_length = 
        (SELECT MAX(streak_length)
        FROM streak_lengths)
    GROUP BY streak_id,streak_start,streak_end
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
# Per month and grand total
duckdb.sql('''
           WITH monthly AS (
                SELECT start_date_local_yyyy_mm, count(*) AS total_runs
                FROM runs_in_2025
                GROUP BY start_date_local_yyyy_mm
                ORDER BY start_date_local_yyyy_mm
            )
            , total AS (
                SELECT 'Grand total', count(*) AS total_runs
                FROM runs_in_2025
            )
            , monthly_avg AS
                (
                SELECT 'Monthly average',round(count(*)/12,0)
                FROM runs_in_2025
                )
            SELECT * FROM monthly
            UNION ALL
            SELECT * FROM total
            UNION ALL
            SELECT *
            FROM monthly_avg
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

#%%
# What activity had the fastest average pace?
duckdb.sql('''
        SELECT name
            ,date(start_date_local) AS start_date
            ,average_pace_mins_per_mile
            ,distance_miles
            ,kudos_count
        FROM runs_in_2025
        WHERE average_pace_mins_per_mile =
            (SELECT MIN(average_pace_mins_per_mile)
            FROM runs_in_2025
            )
        '''
    )

# %%
# What x activities had the fastest average pace?
x = 5
duckdb.sql(f'''
    WITH ranked_runs AS (
        SELECT
            name
            ,DATE(start_date_local) AS start_date_local
            ,average_pace_mins_per_mile
            ,distance_miles
            ,kudos_count
            ,RANK() OVER (
                ORDER BY average_pace_mins_per_mile ASC
            ) AS pace_rank
        FROM runs_in_2025
    )
    SELECT
        name
        ,start_date_local
        ,average_pace_mins_per_mile
        ,distance_miles
        ,kudos_count
    FROM ranked_runs
    WHERE pace_rank <= {x}
    ORDER BY pace_rank
'''
)


#%%
# What distance did I run on average?
duckdb.sql('''
        SELECT ROUND(AVG(distance_miles),2)
        FROM runs_in_2025
        '''
    )

#%%
# How did average distance change over each quarter?
duckdb.sql('''
        SELECT QUARTER(start_date_local), ROUND(AVG(distance_miles),2) AS avg_distance_miles
        FROM runs_in_2025
        GROUP BY QUARTER(start_date_local)
        ORDER BY QUARTER(start_date_local)
        '''
    )

#%%
# What months did I run the most and which did I run the least in terms of mileage?
duckdb.sql('''
           WITH monthly AS (
                SELECT start_date_local_yyyy_mm, round(sum(distance_miles),2) as total_miles_running
                FROM runs_in_2025
                GROUP BY start_date_local_yyyy_mm
                ORDER BY start_date_local_yyyy_mm
            )
            , maximum AS (
                SELECT *
                FROM monthly
                ORDER BY total_miles_running DESC
                LIMIT 1
            )
            , minimum AS (
                SELECT *
                FROM monthly
                ORDER BY total_miles_running ASC
                LIMIT 1
            )
            SELECT * FROM maximum
            UNION ALL
            SELECT * FROM minimum
            '''
   )

# %%
