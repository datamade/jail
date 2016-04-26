import scrapelib

from .parser import parse_page

BASE_URL = 'http://www2.cookcountysheriff.org/search2/details.asp?jailnumber='
SCRAPER = scrapelib.Scraper(requests_per_minute=60)

if __name__ == '__main__':
    import psycopg2
    
    
    con = psycopg2.connect(database="arrests")
    c = con.cursor()

    while True:
        c.execute("SELECT inmate_id "
                  "FROM (SELECT DISTINCT ON (inmate_id) "
                  "      inmate_id, status, checked "
                  "      FROM poll "
                  "      ORDER BY inmate_id, checked DESC) as last_checked "
                  "WHERE status = 200 "
                  "ORDER BY checked ASC")
        for row in c.fetchall():
            
            inmate_id = row[0]
            print(inmate_id)
            try:
                report = SCRAPER.get(BASE_URL + inmate_id)
            except scrapelib.HTTPError as e:
                if e.response.status_code == 500:
                    c.execute("INSERT INTO poll "
                              "(inmate_id, status, checked) "
                              "VALUES "
                              "(%s, 500, now())",
                              (inmate_id,))
                    con.commit()
                else:
                    raise
            else:
                c.execute("INSERT INTO poll "
                          "(inmate_id, status, checked) "
                          "VALUES "
                          "(%s, 200, now()) "
                          "RETURNING poll_id",
                          (inmate_id,))

                inmate = parse_page(report)

                inmate['poll id'] = c.fetchone()[0]


                if inmate['charges']:
                    c.execute("SELECT * "
                              "FROM inmate_charges "
                              "WHERE inmate_id = %s "
                              "    AND statute=%s",
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
                              "FROM inmate_bond "
                              "WHERE inmate_id = %(id)s "
                              "    AND amount = %(bail amount)s ",
                              inmate)
                    if c.fetchone() is None:
                        c.execute("INSERT INTO inmate_bond "
                                  "VALUES "
                                  "(%(poll id)s, %(id)s, %(bail amount)s)",
                                  inmate)
                elif inmate['bail status']:
                    c.execute("SELECT * "
                              "FROM inmate_bond "
                              "WHERE inmate_id = %(id)s "
                              "    AND status = %(bail status)s",
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
                              "    AND date = %(next court date)s "
                              "    AND location = %(courthouse location)s",
                              inmate)
                    if c.fetchone() is None:
                        c.execute("INSERT INTO court_date "
                                  "VALUES "
                                  "(%(poll id)s, %(id)s, %(next court date)s, %(courthouse location)s)",
                                  inmate)

                if not inmate['visiting information'].startswith('Call for Visit Info'):
                    c.execute("SELECT * "
                              "FROM visitation "
                              "WHERE inmate_id = %(id)s "
                              "    AND visitation = %(visiting information)s",
                              inmate)
                    if c.fetchone() is None:
                        c.execute("INSERT INTO visitation "
                                  "VALUES "
                                  "(%(poll id)s, %(id)s, %(visiting information)s)",
                                  inmate)
                        
                    
                if inmate['housing location']:
                    c.execute("SELECT * "
                              "FROM jail_location "
                              "WHERE inmate_id = %(id)s "
                              "    AND location = %(housing location)s",
                              inmate)
                    if c.fetchone() is None:
                        c.execute("INSERT INTO jail_location "
                                  "VALUES "
                                  "(%(poll id)s, %(id)s, %(housing location)s)",
                                  inmate)
                        

                    
                    
                con.commit()

                          
            
    con.close()
