import datetime
import time
import itertools

import scrapelib
import pytz

from .parser import parse_page
from .poll import load_inmate

BASE_URL = 'http://www2.cookcountysheriff.org/search2/details.asp?jailnumber='
SCRAPER = scrapelib.Scraper(requests_per_minute=60)
CT = pytz.timezone('America/Chicago')

def skip_missing(base_url, max_missing, start_count=1, retries=0):
    if retries < max_missing :
        for i in itertools.count(start_count):
            try:
                yield i, SCRAPER.get(base_url % i)
                retries = 0
            except scrapelib.HTTPError as e:
                if e.response.status_code == 500:
                    restart_count = i + 1
                    break
                else:
                    raise

        yield from skip_missing(base_url, max_missing, restart_count, retries + 1)

def reports(max_missing) :
    current_day = None
    while True:
        today = datetime.datetime.now(CT).date()
        if today != current_day:
            current_day = today
            jail_number = today.strftime('%Y-%m%d') + "%03d"
            base_url = BASE_URL + jail_number
            start_count = 0

        for i, page in skip_missing(base_url, max_missing, start_count+1):
            start_count = i
            yield page

        time.sleep(600)


def inmates(max_missing):
    for report in reports(max_missing):
        yield parse_page(report)
        
if __name__ == '__main__':
    import psycopg2
    from raven import Client
    from .sentry import DSN
    client = Client(DSN)

    cache = scrapelib.cache.FileCache('_cache')        
    SCRAPER.cache_storage = cache
    SCRAPER.cache_write_only = False

    con = psycopg2.connect(database="arrests")
    c = con.cursor()

    for inmate in inmates(max_missing=20):
        print(inmate)
        c.execute("INSERT INTO inmate "
                  "VALUES "
                  "(%(id)s, %(name)s, %(dob)s, %(race)s, %(sex)s, "
                  "%(height)s, %(weight)s, %(booked date)s) "
                  "ON CONFLICT DO NOTHING",
                  inmate)
        
        c.execute("INSERT INTO poll "
                  "(inmate_id, status, checked) "
                  "VALUES "
                  "(%(id)s, 200, now()) "
                  "RETURNING poll_id",
                  inmate)

        poll_id = c.fetchone()[0]

        con.commit()

        load_inmate(c, poll_id, inmate)

        con.commit()


    con.close()
