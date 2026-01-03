#%%
import strava_year_in_sport
import pandas as pd
import matplotlib.pyplot as plt
from pandas.plotting import scatter_matrix
import numpy as np
import seaborn as sns

#%%
staging = strava_year_in_sport.staging
df = staging.df()

# %%
# Ensure datetime
df['start_date_local'] = pd.to_datetime(df['start_date_local'])

# Optional: focus on runs only and filter to 2025
df_runs = df[df['sport_type'] == 'Run'].copy()
df_runs_2025 = df_runs[df_runs['start_date_local'].dt.year == 2025].copy()

plt.rcParams['figure.figsize'] = (10, 6)

# %%
# Plotting everyone
vars_to_plot = [
    'average_pace_mins_per_mile'
    ,'average_heartrate'
    ,'average_cadence'
    ,'distance_miles'
    ,'total_elevation_gain_feet'
]

subset = df_runs_2025[vars_to_plot].dropna()

axes = scatter_matrix(
    subset,
    figsize=(12, 12),
    diagonal='hist',
    alpha=0.6
)

# Loop through axes and add trend lines
for i, x_col in enumerate(vars_to_plot):
    for j, y_col in enumerate(vars_to_plot):

        # Only off-diagonal scatter plots
        if i != j:
            ax = axes[j, i]  # NOTE: row = y, col = x

            x = subset[x_col]
            y = subset[y_col]

            coef = np.polyfit(x, y, 1)
            trend = np.poly1d(coef)

            ax.plot(x, trend(x))

for i, col in enumerate(vars_to_plot):
    if col == 'average_pace_mins_per_mile':
        # invert y-axis where pace is y
        for ax in axes[i, :]:
            ax.invert_yaxis()

        # invert x-axis where pace is x
        for ax in axes[:, i]:
            ax.invert_xaxis()

plt.suptitle('Scatter Matrix of Running Metrics', y=1.02)
plt.show()

# %%
# Avg Pace over Time
subset = df_runs_2025[df_runs_2025['average_pace_mins_per_mile'].notna()]

plt.figure()
plt.plot(
    subset['start_date_local'],
    subset['average_pace_mins_per_mile'],
    marker='o',
    linestyle='-'
)
plt.xlabel('Date')
plt.ylabel('Average Pace (min/mi)')
plt.title('Average Pace Over Time')

plt.show()

# %%
# Weekly Mileage
weekly_miles = (
    df_runs_2025
    .set_index('start_date_local')
    .resample('W')['distance_miles']
    .sum()
)

plt.figure()
plt.plot(
    weekly_miles.index,
    weekly_miles.values,
    marker='o'
)

for i, (x, y) in enumerate(zip(weekly_miles.index, weekly_miles.values)):
    if i % 4 == 0:  # every 4 weeks
        plt.annotate(
            f'{y:.0f}',
            xy=(x, y),              # point location
            xytext=(0, 6),          # offset (x, y) in points
            textcoords='offset points',
            ha='center',
            fontsize=12
        )

plt.xlabel('Week')    
plt.ylabel('Miles')
plt.title('Weekly Mileage')
plt.show()

# %%
# Cumulative mileage
df_sorted = df_runs_2025.sort_values('start_date_local').copy()
df_sorted['cumulative_miles'] = df_sorted['distance_miles'].cumsum()

plt.figure()
plt.plot(
    df_sorted['start_date_local'],
    df_sorted['cumulative_miles']
)
plt.xlabel('Date')
plt.ylabel('Cumulative Miles')
plt.title('Cumulative Running Mileage')
plt.show()

#%%
# heatmap
# Daily total distance
daily = (
    df_runs_2025
    .groupby(df_runs_2025['start_date_local'].dt.date)['distance_miles']
    .sum()
    .reset_index()
)

daily['start_date_local'] = pd.to_datetime(daily['start_date_local'])
daily['week'] = daily['start_date_local'].dt.isocalendar().week
daily['year'] = daily['start_date_local'].dt.year
daily['dow'] = daily['start_date_local'].dt.weekday  # Mon=0

heatmap_df = daily.pivot_table(
    index='dow',
    columns='week',
    values='distance_miles',
    aggfunc='sum'
)

#%%
plt.figure(figsize=(15, 4))
sns.heatmap(
    heatmap_df,
    cmap='Greens',
    linewidths=0.5,
    square=True,
    linecolor="white",
    cbar_kws={"label": "Miles", "shrink": 0.6},
    vmin=0
)

plt.yticks(
    ticks=[0.5,1.5,2.5,3.5,4.5,5.5,6.5],
    labels=['Mon','Tue','Wed','Thu','Fri','Sat','Sun'],
    rotation=0
)

plt.xlabel('Week of Year')
plt.ylabel('Day of Week')
plt.title('Running Distance Heatmap')
plt.show()

# %%
