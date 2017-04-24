""" Process H5 files into one CSV
    Requires multicore computer
    with 16 GB or more of RAM
"""

import os
import h5py
import pandas as pd
import pickle
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("year", type=int)
args = parser.parse_args()

def extract_h5 (loc,art,title):
    """ Extract H5 file into dictionary """
    f = h5py.File(loc, "r")
    features = {}
    for key in f.keys():
        primary_key = f.get(key)
        for sub_key in primary_key.keys():
            size = primary_key.get(sub_key).shape[0]
            tmp = []
            for i in range(0, size):
                tmp.append(primary_key.get(sub_key)[i])
            features[sub_key] = tmp
    features['artist_name'] = art.lower().strip()
    features['song'] = title.lower().strip()
    return features


if __name__ == '__main__':
    """ Multithread H5 file processing"""
    training_file = 'training_songs_year.csv'
    hits_file = 'final_top_hits.pik'
    year = args.year

    songs = pd.read_csv(training_file, encoding = "ISO-8859-1")
    songs = songs[songs['year'] == year]

    row_iterator = songs.iterrows()
    rows = []
    for i, row in row_iterator:
        rows.append([row['location'], row['artist_name'], row['title']])

    import multiprocessing as mp
    pool = mp.Pool(processes=8)
    results = [pool.apply_async(extract_h5, args=(x[0],x[1],x[2])) for x in rows]
    df = pd.DataFrame()
    for p in results:
        df = df.append(p.get(), ignore_index=True)
    df.to_csv("data_" + str(year) + ".csv")