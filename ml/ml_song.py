import functools
import random
import math
import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier, export_graphviz
from sklearn import svm
from sklearn.svm import SVR
from sklearn.pipeline import Pipeline
from sklearn.neural_network import MLPClassifier

# Variables

# File prefix
file_pre = 'normw_data_'

# File suffix
file_suf = '.csv'

# Start year
start = 1990

# Number of years to process
end = 21 

# Number of iterations
runs = 5

# Training/Test percent split
training_percent = 0.85

# ML Method
# 'dt' - decision tree
# 'svm' - SVM
# 'mlp' - MLP
method = 'dt'

# Create file list
all_files = []
for i in range(end+1):
    all_files.append(file_pre + str(start + i) + file_suf)

random.shuffle(all_files)

# Need data for if hit or not

hits = pd.read_csv('labelled_data.csv', encoding = "ISO-8859-1")
hits['song'] = hits['title'].str.lower() 
hits['artist_name'] = hits['artist_name'].str.lower() 

# Functions for Metrics
def acc(data):
    in_ac = [float(1) if a==b else 0 for (a,b) in data]
    return functools.reduce(lambda x, y: x + y, in_ac) / len(in_ac)
def pr(data):
    l = len(data)
    tp = functools.reduce(lambda x, y: x + y, [float(1) if a==b else 0 for (a,b) in data])
    fp = functools.reduce(lambda x, y: x + y, [float(1) if a==0 and p==1 else 0 for (a,p) in data])
    fn = functools.reduce(lambda x, y: x + y, [float(1) if a==1 and p==0 else 0 for (a,p) in data])
    
    pr = tp / (tp + fp)
    rc = tp / (tp + fn)
    return (pr, rc)

# Ingest files, randomize order, train model, predict for test set, and score.

for i in range(runs):
    print ("Run #" + str(i))
    print ("Method: " + method)
    # Ingest and randomize row order
    random.shuffle(all_files)
    df = pd.DataFrame()
    for file in all_files:
        chunks = pd.read_csv(file, encoding = "ISO-8859-1", chunksize=1000, low_memory=False,
                           usecols=['artist_name', 'song', 'artist_nid','song_nid', 'bars', 'beats',
                                    'sections_start', 'segments', 'segments_loudness_max', 'tatums'])
        tmp = pd.concat((chunk for chunk in chunks), ignore_index=True)
        df = pd.concat([df, tmp], ignore_index=True)
        
    df = pd.merge(df, hits, on=['song', 'artist_name'], how='inner').reindex(np.random.permutation(df.index))
    
    # Split training/testing dataset
    train_num = math.ceil (training_percent * len(df.index))
    test_num = len(df.index) - train_num
    train_df = df.head(train_num)
    test_df = df.tail(test_num)
    
    # Train Model
    if method == 'dt':
        clf = DecisionTreeClassifier(min_samples_split=100, min_samples_leaf=100, max_depth=4, max_leaf_nodes=15)
    elif method == 'svm':
        # Set max_iter=100 to speed up processing and model convergence  
        clf = svm.SVC(kernel='rbf', C=1e3, gamma=0.001, cache_size=6000, max_iter=100)
    else:
        clf = MLPClassifier(solver='lbfgs', alpha=1e-5, hidden_layer_sizes=(6), random_state=1)
        
    y = train_df["hit"] 
    X = train_df[['bars', 'beats', 'sections_start', 'segments', 'segments_loudness_max', 'tatums']]
    clf.fit(X, y)
    
    # Predict and score
    sample_size = 0 # Use whole test data set
    sample = test_df
    if sample_size:
        sample = sample.sample(sample_size)
    results = []
    for index, row in sample.iterrows():
        tmp_df = pd.DataFrame([row])
        pred = clf.predict(tmp_df[['bars', 'beats', 'sections_start', 'segments', 'segments_loudness_max', 'tatums']])
        results.append((row['hit'], pred[0]))
    print ("acc: " + str(acc(results)))
    print ("precision/recall:" + str(pr(results)))
    
    if method == 'dt':
        export_graphviz(dt, out_file='dt' + str(i) + '.dot')

