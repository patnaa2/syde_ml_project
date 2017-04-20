from collections import defaultdict
import cPickle as pickle
import csv
import re

def main():
    artists = {}
    artists.update(pickle.load(open("missing_artists.pik")))

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

    matched = 0

    for song, artist in artists.iteritems():
        if 'featuring' in artist:
            artists = [x.strip() for x in artist.split('featuring')]
        elif '&' in artist:
            artists = [x.strip() for x in artist.split('&')]
        elif 'and' in artist:
            artists = [x.strip() for x in artist.split('and')]
        else:
            artists = [artist]

        artists = [re.sub('[^A-Za-z0-9]+', '', x) for x in artists]

        for row in csv_data[song]:
            for a in artists:
                csv_artist = re.sub('[^A-Za-z0-9]+', '', row[5])
                if a in csv_artist and row[-1] == '0':
                    row[-1] = 1

    # dump the csv again
    with open('updated_data.csv', 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(title)
        for rows in csv_data.itervalues():
            for row in rows:
                writer.writerow(row)


if __name__ == "__main__":
    main()
