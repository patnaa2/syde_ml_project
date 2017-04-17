from bs4 import BeautifulSoup
from datetime import timedelta, date
from random import uniform
from threading import Thread, Lock
import urllib2
import cPickle as pickle
import time

PRINT_LOCK = Lock()

class ChartScraper(Thread):
    HEADER_URL = "http://www.billboard.com/charts/hot-100/"

    def __init__(self, date):
        self.date = date
        self.url = self.HEADER_URL + date.strftime("%Y-%m-%d")
        self.songs = {}
        self.failed = False
        self.e = None
        super(ChartScraper, self).__init__()

    def run(self):
        with PRINT_LOCK:
            print "Processing data for %s" %(self.date)
        try:
            page = urllib2.urlopen(self.url)
        except Exception as e:
            self.failed = True
            self.e = str(e)

        soup = BeautifulSoup(page, "lxml")

        songs = soup.find_all("h2", {"class": "chart-row__song"})

        assert len(songs) == 100

        for song in songs:
            artist = [x for x in song.parent][3].text.strip()
            self.songs[str(song.text)] = str(artist)

def date_gen(start_date, end_date, n=7):
    assert type(start_date) == date
    assert type(end_date) == date

    if start_date.weekday() != 5:
        print "Specified start date %s is not a Saturday" %(start_date)
        if start_date.weekday() == 6:
            start_date += timedelta(6)
        else:
            start_date += timedelta(5 - start_date.weekday())
        print "Updating start date to next Saturday %s" %(start_date)

    if start_date > end_date:
        raise StopIteration

    while True:
        yield start_date
        start_date += timedelta(n)
        if start_date > end_date:
            raise StopIteration

def main():
    MAX_THREADS = 15
    SLEEP = 1
    FILE_NAME = "top_hits_1990s.pik"
    START_DATE = date(1990, 01, 01)
    END_DATE = date(2000, 01, 01)
    CHECKPOINT_INTERVAL = 10

    songs = {}
    scraper_threads = []
    date_iter = date_gen(START_DATE, END_DATE)
    jitter_power = 1
    batch = 0
    total_batches = (END_DATE - START_DATE).days / 7 / MAX_THREADS
    stop_iter = False

    while True:
        print "Processing batch %d/%d" %(batch, total_batches)
        for _ in xrange(MAX_THREADS):
            try:
                process_date = date_iter.next()
            except Exception as e:
                print e
                stop_iter = True

            t = ChartScraper(process_date)
            t.start()
            scraper_threads.append(t)

        for t in scraper_threads:
            t.join()
            # we are thread safe here
            if t.failed:
                print "Failed Thread: %s" %(t.e)
                import pdb ; pdb.set_trace()
            songs.update(t.songs)

        scraper_threads = []

        if stop_iter:
            break

        # checkpoint the dictionary so we don't have to keep redoing this
        # suspcious we are being rate limited here
        if not (batch % CHECKPOINT_INTERVAL):
            print "Checkpointing...\n"
            CHECKPOINT_FILE = "%s_%s" %(FILE_NAME, process_date)
            pickle.dump(songs, open(CHECKPOINT_FILE, "wb"))


        # sleep w jitter
        print "Scraped one batch..sleeping and continuing"
        batch += 1
        time.sleep(SLEEP +  (uniform(-1, 1) * jitter_power))

    # pickle the data for the last time
    pickle.dump(songs, open(FILE_NAME, "wb"))
    print "Done. Data has been saved as a dict in %s" %(FILE_NAME)
    return 0

if __name__ == "__main__":
    main()
