import scrapelib

from .parser import parse_page

BASE_URL = 'http://www2.cookcountysheriff.org/search2/details.asp?jailnumber='
SCRAPER = scrapelib.Scraper(requests_per_minute=120)

def load_inmate(c, poll_id, inmate):
    inmate['poll id'] = poll_id


    if inmate['charges']:
        c.execute("SELECT * "
                  "FROM (SELECT DISTINCT ON (inmate_id) * "
                  "      FROM inmate_charges "
                  "      WHERE inmate_id = %s "
                  "      ORDER BY inmate_id, poll_id DESC) last_charge "
                  "WHERE statute=%s",
                  (inmate['id'],
                   inmate['charges'][0]))
        if c.fetchone() is None:
            c.execute("INSERT INTO inmate_charges "
                      "VALUES "
                      "(%s, %s, %s, %s)",
                      (inmate['poll id'],
                       inmate['id'],
                       inmate['charges'][0],
                       inmate['charges'][1]))

    if inmate['bail amount']:
        c.execute("SELECT * "
                  "FROM (SELECT DISTINCT ON (inmate_id) * "
                  "      FROM inmate_bond "
                  "      WHERE inmate_id = %(id)s "
                  "      ORDER BY inmate_id, poll_id DESC) last_bond "
                  "WHERE amount = %(bail amount)s",
                  inmate)
        if c.fetchone() is None:
            c.execute("INSERT INTO inmate_bond "
                      "VALUES "
                      "(%(poll id)s, %(id)s, %(bail amount)s)",
                      inmate)
    elif inmate['bail status']:
        c.execute("SELECT * "
                  "FROM (SELECT DISTINCT ON (inmate_id) * "
                  "      FROM inmate_bond "
                  "      WHERE inmate_id = %(id)s "
                  "      ORDER BY inmate_id, poll_id DESC) last_bond_status "
                  "WHERE status = %(bail status)s",
                  inmate)
        if c.fetchone() is None:
            c.execute("INSERT INTO inmate_bond "
                      "(poll_id, inmate_id, status) "
                      "VALUES "
                      "(%(poll id)s, %(id)s, %(bail status)s)",
                      inmate)

    if inmate['next court date']:
        c.execute("SELECT * "
                  "FROM court_date "
                  "WHERE inmate_id = %(id)s "
                  "  AND date = %(next court date)s "
                  "  AND location = %(courthouse location)s",
                  inmate)
        if c.fetchone() is None:
            c.execute("INSERT INTO court_date "
                      "VALUES "
                      "(%(poll id)s, %(id)s, %(next court date)s, %(courthouse location)s)",
                      inmate)

    if not inmate['visiting information'].startswith('Call for Visit Info'):
        c.execute("SELECT * "
                  "FROM (SELECT DISTINCT ON (inmate_id) * "
                  "      FROM visitation "
                  "      WHERE inmate_id = %(id)s "
                  "      ORDER BY inmate_id, poll_id DESC) last_visiting_info "
                  "WHERE visitation = %(visiting information)s",
                  inmate)
        if c.fetchone() is None:
            c.execute("INSERT INTO visitation "
                      "VALUES "
                      "(%(poll id)s, %(id)s, %(visiting information)s)",
                      inmate)


    if inmate['housing location']:
        c.execute("SELECT * "
                  "FROM (SELECT DISTINCT ON (inmate_id) * "
                  "      FROM jail_location "
                  "      WHERE inmate_id = %(id)s "
                  "      ORDER BY inmate_id, poll_id DESC) last_housing "
                  "WHERE location = %(housing location)s",
                  inmate)
        if c.fetchone() is None:
            c.execute("INSERT INTO jail_location "
                      "VALUES "
                      "(%(poll id)s, %(id)s, %(housing location)s)",
                      inmate)

def interleave_priority(all_records, c):
    i = 0
    recent_records = set()
    while all_records:
        if not recent_records:
            c.execute("SELECT inmate_id "
                      "FROM poll "
                      "INNER JOIN inmate_bond "
                      "USING (inmate_id) "
                      "GROUP BY inmate_id "
                      "HAVING BOOL_AND(poll.status=200) "
                      "   AND BOOL_AND(inmate_bond.status = '*NO BOND*') "
                      "   AND NOW() - MIN(checked) < INTERVAL '1 days'")
            recent_records = {row[0] for row in c}
            all_records -= recent_records

        if i % 2 == 0 and recent_records:
            inmate_id = recent_records.pop()
        else:
            inmate_id = all_records.pop()

        yield inmate_id

        i += 1

if __name__ == '__main__':
    import psycopg2
    try:
        from raven import Client
        from .sentry import DSN
        client = Client(DSN)
    except ImportError:
        pass
    
    con = psycopg2.connect(database="arrests")
    c = con.cursor()

    while True:
        c.execute("SELECT inmate_id "
                  "FROM poll "
                  "GROUP BY inmate_id "
                  "HAVING BOOL_AND(status = 200)")

        all_records = {row[0] for row in c.fetchall()}
        for inmate_id in interleave_priority(all_records, c):
            
            print(inmate_id)

            inmate_present = None

            try:
                report = SCRAPER.get(BASE_URL + inmate_id)
            except scrapelib.HTTPError as e:
                if e.response.status_code == 500:
                    inmate_present = False
                else:
                    client.captureException()
                    raise
            else:
                inmate_present = True

            c.execute("INSERT INTO poll "
                      "(inmate_id, status, checked) "
                      "VALUES "
                      "(%s, %s, now()) "
                      "RETURNING poll_id",
                      (inmate_id, 200 if inmate_present else 500))
            con.commit()

            if not inmate_present:
                continue

            poll_id = c.fetchone()[0]
            inmate = parse_page(report)

            load_inmate(c, poll_id, inmate)

            con.commit()
            
    con.close()
