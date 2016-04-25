PG_DB=arrests
define check_relation
 psql -d $(PG_DB) -c "\d $@" > /dev/null 2>&1 ||
endef

.PHONY : tables

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

