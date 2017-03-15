from bs4 import BeautifulSoup
from datetime import timedelta, date
from random import uniform
from threading import Thread
import urllib2
import cPickle as pickle
import time

class ChartScraper(Thread):
    HEADER_URL = "http://www.billboard.com/charts/hot-100/"

    def __init__(self, date):
        self.url = self.HEADER_URL + date.strftime("%Y-%m-%d")
        self.songs = {}
        self.failed = False
        self.e = None
        super(ChartScraper, self).__init__()

    def run(self):
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

    if start_date > end_date:
        raise StopIteration

    while True:
        yield start_date
        start_date += timedelta(n)
        if start_date > end_date:
            raise StopIteration

def main():
    MAX_THREADS = 5
    FILE_NAME = "top_hits.p"
    START_DATE = date(1997, 02, 01)
    END_DATE = date(1997, 02, 16)

    songs = {}
    scraper_threads = []
    date_iter = date_gen(START_DATE, END_DATE)
    jitter_power = 1
    batch = 1
    total_batches = (END_DATE - START_DATE).days / 7 / MAX_THREADS
    stop_iter = False

    while True:
        print "Processing batch %d/%d" %(batch, total_batches)
        for _ in xrange(MAX_THREADS):
            try:
                process_date = date_iter.next()
            except:
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

        print "Scraped one batch..sleeping and continuing"

        if stop_iter:
            break

        # sleep w jitter
        time.sleep(1 +  (uniform(-1, 1) * jitter_power))

    # pickle the data
    pickle.dump(songs, open(FILE_NAME, "wb"))
    print "Done. Data has been saved as a dict in %s" %(FILE_NAME)
    return 0

if __name__ == "__main__":
    main()
