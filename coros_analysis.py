#%%
from fitparse import FitFile
import glob
from pathlib import Path
import os
import logging
import pandas as pd
import datetime
from datetime import datetime
import time

#%%
print("Grabbing Coros data")

fit_subpath = "coros_data"
DATA_DIR = Path(os.path.join(os.path.dirname(__file__), fit_subpath))

#%%
# Get all the fit files
fit_files = list(DATA_DIR.glob("*.fit"))
fit_file_count = len(fit_files)

#%%
print(f"Converting {fit_file_count} FIT files to CSVs.")

records = []
skipped = 0
start_time = datetime.now()

for fit_path in fit_files:
    try:
        fitfile = FitFile(str(fit_path), check_crc=False)

        for record in fitfile.get_messages("record"):
            row = {}
            for field in record:
                row[field.name] = field.value

            row["source_file"] = fit_path.name
            records.append(row)

    except Exception as e:
        #print(f"Skipping {fit_path.name}: {e}")
        skipped += 1

end_time = datetime.now()
runtime = round(int((end_time - start_time).total_seconds())/60,2)

print(f"Completed FIT file parsing in {runtime} minutes")
print(f"Skipped {skipped} files out of {fit_file_count}")

#%%
df = pd.DataFrame(records)
df.to_csv("coros_data_combined.csv", index=False)
