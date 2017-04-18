from collections import defaultdict
import cPickle as pickle
import csv

def main():
    billboard_data = {}

    # load 1990s data
    billboard_data.update(pickle.load(open("top_hits_1990s.pik")))

    # load 2000s data
    billboard_data.update(pickle.load(open("top_hits_2000s.pik")))

    csv_data = defaultdict(list)
    title = []
    count = 0

    # load csv data
    with open("data.csv") as csvfile:
        reader = csv.reader(csvfile)

        for row in reader:
            # title row
            if count == 0:
                title.extend(row)
                count += 1
                continue

            count += 1
            song_title = row[1].lower()
            csv_data[song_title].append(row)

    match = 0
    for song, rows in csv_data.iteritems():
        if song in billboard_data:
            for row in rows:
                if row[5].lower() == billboard_data[song]:
                    row.append(1)
                    match += 1
                else:
                    row.append(0)
        else:
            for row in rows:
                row.append(1)

    rows_written = 0
    with open('updated_data.csv', 'wb') as csvfile:
        writer = csv.writer(csvfile)
        title.append('hit')
        writer.writerow(title)
        for rows in csv_data.itervalues():
            for row in rows:
                rows_written += 1
                writer.writerow(row)

    print "Labelled %s/%s as hits." %(match, rows_written)

if __name__ == "__main__":
    main()
