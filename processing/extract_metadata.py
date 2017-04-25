import os
import h5py
import sqlite3
import pandas as pd

# Variables

track_metadata = "D:\\522\\AdditionalFiles\\track_metadata.db"
artist_term = "D:\\522\\AdditionalFiles\\artist_term.db"
search_dir = "D:\\522\\data"


# Import track metadata
con = sqlite3.connect(track_metadata)
metadata_df = pd.read_sql_query("SELECT track_id,title,release,song_id,artist_id,artist_name,artist_mbid,year from songs", con)
print(metadata_df.head())
con.close()

# Import artist term
con = sqlite3.connect(artist_term)
artist_df = pd.read_sql_query("SELECT * from artist_term", con)
print(artist_df.head())
con.close()

# Join between metadata and artist terms
merged_df = pd.merge(metadata_df, artist_df, on=["artist_id"], how='inner')

# Filter category = 'pop'
filtered_df = merged_df[merged_df['term'] == ("pop")]
filtered_df = filtered_df[filtered_df['year'] > 1989]

# Search for H5 data files on hdd
# Dataset comes with songs in hundreds of different folders but can't find a database that
# links tracks to folder locations. Must manually do this
# Its a lengthy process so recommend saving the df to hdd for caching

h5_files = {}
for root, dirs, files in os.walk(search_dir):
    for file in files:
        if file.endswith(".h5"):
             h5_files[file.split('\\')[-1][:-3]]=os.path.join(root, file)

# Convert dict to DF
cache_location = pd.DataFrame([[key,value] for key,value in h5_files.items()],columns=["track_id","location"])

# Save to file
cache_location.to_csv("cache_location.csv")

# Read DF in
cache_location = pd.read_csv('cache_location.csv')

# List of songs that should be analyzed
songs_to_copy = pd.merge(filtered_df, cache_location, on=['track_id'], how='inner').drop_duplicates(subset=['location'])

# Python get rid of special characters
types = songs_to_copy.apply(lambda x: pd.lib.infer_dtype(x.values))
for col in types[types=='unicode'].index:
    songs_to_copy[col] = songs_to_copy[col].apply(lambda x: x.replace('[^\x00-\x7F]','').decode('unicode_escape').encode('ascii', 'ignore').lower().strip())

songs_to_copy.to_csv("song_metadata.csv")
