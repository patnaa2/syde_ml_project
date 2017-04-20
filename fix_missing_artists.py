from collections import defaultdict
import cPickle as pickle
import csv
import re

def main():
    songs = {}

    songs.update(pickle.load(open("missed_songs_90s.pik")))
    songs.update(pickle.load(open("missed_songs_20s.pik")))

    csv_data = defaultdict(list)
    title = None
    count = 0

    # load csv data
    with open("labelled_data.csv") as csvfile:
        reader = csv.reader(csvfile)

        for row in reader:
            # title row
            if count == 0:
                title = row
                count += 1
                continue

            count += 1
            song_title = re.sub('[^A-Za-z0-9]+', '', row[1].lower())
            csv_data[song_title].append(row)

    songs = { re.sub('[^A-Za-z0-9]+', '', k) : v for k, v in songs.iteritems()}

    matched = set()
    matched_songs = 0
    unmatched_songs = []

    for song, artist in songs.iteritems():
        if song in csv_data:
            match_bool = False
            matched_songs += 1
            # massage the artists a bit
            artist = artist.lower()
            if '&' in artist:
                a = [x.strip() for x in artist.split('&')]
            elif 'and' in artist:
                a = [x.strip() for x in artist.split('and')]
            elif 'featuring' in artist:
                a = [x.strip() for x in artist.split('featuring')]
            else:
                a = [artist]

            for row in csv_data[song]:
                for art in a:
                    if art in row[5].lower():
                        if row[-1] == 0:
                            import pdb ; pdb.set_trace()
                        row[-1] = 1
                        matched.add(song)
                        match_bool = True

            if not match_bool:
                unmatched_songs.append(song)

    # dump the csv again
    with open('updated_data_1.csv', 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(title)
        for rows in csv_data.itervalues():
            for row in rows:
                writer.writerow(row)

    # remove songs that have been matched
    songs = {k:v for k, v in songs.iteritems() if k not in matched}
    pickle.dump(songs, open("unmatched_songs", "wb"))

    # dump unmatched songs
    pickle.dump(unmatched_songs, open("unmatched_songs_w_artist.pik", "wb"))

    # also dumping the dict from above
    pickle.dump(csv_data, open("csv_data.pik", "wb"))

if __name__ == "__main__":
    main()
