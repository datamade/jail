PG_DB=arrests
define check_relation
 psql -d $(PG_DB) -c "\d $@" > /dev/null 2>&1 ||
endef

.PHONY : tables
tables: inmate poll inmate_bond inmate_charges court_date jail_location visitation

inmate :
	$(check_relation) psql -d $(PG_DB) -c \
            "CREATE TABLE $@ (inmate_id TEXT PRIMARY KEY, \
                              name TEXT, \
                              date_of_birth DATE, \
                              race TEXT, \
                              sex TEXT, \
                              height INT, \
                              weight INT, \
                              booked_date DATE, \
                              _created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)"

poll :
	$(check_relation) psql -d $(PG_DB) -c \
            "CREATE TABLE $@ (poll_id SERIAL PRIMARY KEY, \
                              inmate_id TEXT, \
                              status INT, \
                              checked TIMESTAMPTZ)"

inmate_bond :
	$(check_relation) psql -d $(PG_DB) -c \
	    "CREATE TABLE $@ (poll_id INT, \
                              inmate_id TEXT, \
                              amount NUMERIC, \
                              status TEXT)"

inmate_charges :
	$(check_relation) psql -d $(PG_DB) -c \
	    "CREATE TABLE $@ (poll_id INT, \
                              inmate_id TEXT, \
                              statute TEXT, \
                              description TEXT)"

court_date :
	$(check_relation) psql -d $(PG_DB) -c \
            "CREATE TABLE $@ (poll_id INT, \
                              inmate_id TEXT, \
                              date DATE, \
                              location TEXT)"

jail_location :
	$(check_relation) psql -d $(PG_DB) -c \
            "CREATE TABLE $@ (poll_id INT, \
                              inmate_id TEXT, \
                              location TEXT)"

visitation :
	$(check_relation) psql -d $(PG_DB) -c \
            "CREATE TABLE $@ (poll_id INT, \
                              inmate_id TEXT, \
                              visitation TEXT)"
