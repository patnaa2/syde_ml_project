import functools
import sqlite3
import pandas as pd

# Variables

# Preprocessed files
file_list = ['data_2008.csv', 'data_2009.csv', 'data_2010.csv', 'data_2011.csv']

# Metadata DB
track_metadata = "D:\\522\\AdditionalFiles\\track_metadata.db"

# Normalization and dimensionality reduction functions
# Note eval str() must be used because of string storage format
def avg(r):
    """ Returns average of list"""
    z = eval(str(r))
    l = len(z)
    if l > 0:
        return functools.reduce(lambda x, y: x + y, z) / l
    return 0

def wavg(w, v):
    """ Returns weighted average of list where w is weight and v is value"""
    zipped = zip(eval(str(w)),eval(str(v)))
    
    top = [ c * x for (c,x) in zipped]
    bot = sum (eval(str(w)))
    if bot:
        return functools.reduce(lambda x, y: x + y, top) / bot
    return 0

def max_norm(r):
    """ Returns max in list"""
    z = eval(str(r))
    if len(z) > 0:
        return functools.reduce(lambda x, y: x if x > y else y, z)
    return 0

# Normalize files in list
for file in file_list:
    print (file)
    chunks = pd.read_csv(file, encoding = "ISO-8859-1", chunksize=500, low_memory=False,
                       usecols=['artist_name', 'artist_terms', 'bars_confidence', 'bars_start', 'beats_confidence',
                                'beats_start',
                                'sections_start', 'segments_confidence', 'segments_loudness_max', 'segments_start',
                                'song', 'tatums_start', 'tatums_confidence'])
    df = pd.concat((chunk for chunk in chunks), ignore_index=True)

    df['bars'] = df.apply(lambda row: wavg(row['bars_confidence'], row['bars_start']), axis=1)
    df.drop('bars_confidence', axis=1, inplace=True)
    df.drop('bars_start', axis=1, inplace=True)
    
    df['beats'] = df.apply(lambda row: wavg(row['beats_confidence'], row['beats_start']), axis=1)
    df.drop('beats_confidence', axis=1, inplace=True)
    df.drop('beats_start', axis=1, inplace=True)

    df['sections_start'] = df['sections_start'].apply(avg)
        
    df['segments'] = df.apply(lambda row: wavg(row['segments_confidence'], row['segments_start']), axis=1)
    df.drop('segments_confidence', axis=1, inplace=True)
    df.drop('segments_start', axis=1, inplace=True)
    
    df['segments_loudness_max'] = df['segments_loudness_max'].apply(max_norm)
    
    df['tatums'] = df.apply(lambda row: wavg(row['tatums_confidence'], row['tatums_start']), axis=1)
    df.drop('tatums_start', axis=1, inplace=True)
    df.drop('tatums_confidence', axis=1, inplace=True)

    df.to_csv("normw_" + file)
