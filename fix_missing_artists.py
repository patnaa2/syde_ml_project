from collections import defaultdict
import cPickle as pickle
import csv

def main():
    artists = {}

    artists.update(pickle.load(open("missing_artists_90s.pik")))
    artists.update(pickle.load(open("missing_artists_20s.pik")))

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
            song_title = row[1].lower()
            csv_data[song_title].append(row)

    matched = set()
    for song, artist in artists.iteritems():
        if 'featuring' in artist:
            a2 = [a.strip() for a in artist.split("featuring")]

            for row in csv_data[song]:
                for ar in a2:
                    if ar in row[5].lower():
                        matched.add(song)
                        row[-1] = 1
			break

    print "Total matched: %s" %(len(matched))

    rows_written = 0
    with open('updated_data_1.csv', 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(title)
        for rows in csv_data.itervalues():
            for row in rows:
                rows_written += 1
                writer.writerow(row)

    artists = {k: v for k,v in artists.iteritems() if k not in matched}
    pickle.dump(artists, open("missing_artists.pik", "wb"))

if __name__ == "__main__":
    main()
