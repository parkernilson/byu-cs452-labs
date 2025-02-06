## This script is used to insert data into the database
import os
import pandas as pd

from utils import fast_pg_insert, show_dims
from db import CONNECTION

# if dataset.csv and podcast_segment.csv are not present, run gen_data_files.py
if not os.path.exists("dataset.csv") or not os.path.exists("podcast_segment.csv"):
    print("dataset.csv or podcast_segment.csv not found. Run gen_data_files.py first")
    exit()

# print("Reading dataset.csv")
# ds_frame = pd.read_csv("dataset.csv")

# print("Inserting podcast data into database")
# podcasts_df = ds_frame[['id', 'title']]
# show_dims(podcasts_df)
# fast_pg_insert(podcasts_df, CONNECTION, 'podcast', ['id', 'title'])

print("Inserting podcast segment data into database")
chunks = pd.read_csv("podcast_segment.csv", chunksize=10000, sep=",")
progress = 0
total = 832839

for chunk in chunks:
    print(f"Inserting chunk of size {chunk.shape}")
    print(f"Progress: {progress}/{total} {progress/total*100:.2f}%")
    fast_pg_insert(chunk, CONNECTION, 'podcast_segment', ['id', 'podcast_id', 'start_time', 'end_time', 'content', 'embedding'])
    progress += chunk.shape[0]

# show_dims(podcasts_segments_df)
# fast_pg_insert(podcasts_segments_df, CONNECTION, 'podcast_segment', ['id', 'podcast_id', 'start_time', 'end_time', 'content', 'embedding'])

print("Data inserted successfully")