from collections import defaultdict
import cPickle as pickle
import csv

def main():
    billboard_data_90 = {}
    billboard_data_20 = {}

    # load 1990s data
    billboard_data_90.update(pickle.load(open("top_hits_1990s.pik")))

    # load 2000s data
    billboard_data_20.update(pickle.load(open("top_hits_2000s.pik")))

    csv_data = defaultdict(list)
    count = 0

    # load csv data
    with open("data.csv") as csvfile:
        reader = csv.reader(csvfile)

        for row in reader:
            # title row
            if count == 0:
                count += 1
                continue

            count += 1
            song_title = row[1].lower()
            csv_data[song_title].append(row)

    def check_for_missing_songs(b_data):
        missed_hits = {}
        missing_artists = {}

        for song, artist in b_data.iteritems():
            if song not in csv_data:
                missed_hits[song] = artist
            else:
                found = False
                for row in csv_data[song]:
                    if row[5].lower() == artist:
                        found = True
                        break

                if not found:
                    missing_artists[song] = artist

        return missed_hits, missing_artists

    missed_hits_90s, missing_artists_90s = check_for_missing_songs(billboard_data_90)
    missed_hits_20s, missing_artists_20s = check_for_missing_songs(billboard_data_20)

    total_90s_misses = len(missed_hits_90s) + len(missing_artists_90s)
    total_20s_misses = len(missed_hits_20s) + len(missing_artists_20s)

    print "Total number of songs found in 90s: %s/%s" %(len(billboard_data_90) - total_90s_misses, len(billboard_data_90))
    print "Total number of songs found in 20s: %s/%s" %(len(billboard_data_20) - total_20s_misses, len(billboard_data_20))

    pickle.dump(missed_hits_90s, open("missed_songs_90s.pik", "wb"))
    pickle.dump(missed_hits_20s, open("missed_songs_20s.pik", "wb"))
    pickle.dump(missing_artists_90s, open("missing_artists_90s.pik", "wb"))
    pickle.dump(missing_artists_20s, open("missing_artists_20s.pik", "wb"))

if __name__ == "__main__":
    main()
