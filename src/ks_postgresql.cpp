#include "endpoint.h"

#include <stdexcept>
#include <set>
#include <libpq-fe.h>

#include "schema.h"
#include "database_client_traits.h"
#include "row_printer.h"

class PostgreSQLRes {
public:
	PostgreSQLRes(PGresult *res);
	~PostgreSQLRes();

	inline PGresult *res() { return _res; }
	inline ExecStatusType status() { return PQresultStatus(_res); }
	inline int n_tuples() const  { return _n_tuples; }
	inline int n_columns() const { return _n_columns; }
	inline Oid type_of(int column_number) const { return types[column_number]; }

private:
	PGresult *_res;
	int _n_tuples;
	int _n_columns;
	vector<Oid> types;
};

PostgreSQLRes::PostgreSQLRes(PGresult *res) {
	_res = res;

	_n_tuples = PQntuples(_res);
	_n_columns = PQnfields(_res);

	types.resize(_n_columns);
	for (size_t i = 0; i < _n_columns; i++) {
		types[i] = PQftype(_res, i);
	}
}

PostgreSQLRes::~PostgreSQLRes() {
	if (_res) {
		PQclear(_res);
	}
}


// from pg_type.h, which isn't available/working on all distributions.
#define BOOLOID			16
#define BYTEAOID		17
#define INT2OID			21
#define INT4OID			23
#define INT8OID			20

class PostgreSQLRow {
public:
	inline PostgreSQLRow(PostgreSQLRes &res, int row_number): _res(res), _row_number(row_number) { }
	inline const PostgreSQLRes &results() const { return _res; }

	inline         int n_columns() const { return _res.n_columns(); }

	inline        bool   null_at(int column_number) const { return PQgetisnull(_res.res(), _row_number, column_number); }
	inline const void *result_at(int column_number) const { return PQgetvalue (_res.res(), _row_number, column_number); }
	inline         int length_of(int column_number) const { return PQgetlength(_res.res(), _row_number, column_number); }
	inline      string string_at(int column_number) const { return string((const char *)result_at(column_number), length_of(column_number)); }
	inline        bool   bool_at(int column_number) const { return (strcmp((const char *)result_at(column_number), "t") == 0); }
	inline     int64_t    int_at(int column_number) const { return strtoll((const char *)result_at(column_number), NULL, 10); }

	string decoded_byte_string_at(int column_number) const;

	template <typename Packer>
	inline void pack_column_into(Packer &packer, int column_number) const {
		if (null_at(column_number)) {
			packer << nullptr;
		} else {
			switch (_res.type_of(column_number)) {
				case BOOLOID:
					packer << bool_at(column_number);
					break;

				case BYTEAOID:
					packer << decoded_byte_string_at(column_number);
					break;

				case INT2OID:
				case INT4OID:
				case INT8OID:
					packer << int_at(column_number);
					break;

				default:
					// we use our non-copied memory class, equivalent to but faster than using string_at
					packer << memory(result_at(column_number), length_of(column_number));
			}
		}
	}

	template <typename Packer>
	void pack_row_into(Packer &packer) const {
		pack_array_length(packer, n_columns());

		for (size_t column_number = 0; column_number < n_columns(); column_number++) {
			pack_column_into(packer, column_number);
		}
	}

private:
	PostgreSQLRes &_res;
	int _row_number;
};

string PostgreSQLRow::decoded_byte_string_at(int column_number) const {
	const unsigned char *value = (const unsigned char *)result_at(column_number);
	size_t decoded_length;
	const unsigned char *decoded = PQunescapeBytea(value, &decoded_length);
	string result(decoded, decoded + decoded_length);
	PQfreemem((void *)decoded);
	return result;
}


class PostgreSQLClient {
public:
	typedef PostgreSQLRow RowType;

	PostgreSQLClient(
		const string &database_host,
		const string &database_port,
		const string &database_name,
		const string &database_username,
		const string &database_password);
	~PostgreSQLClient();

	template <typename RowReceiver>
	size_t retrieve_rows(RowReceiver &row_packer, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, ssize_t row_count = NO_ROW_COUNT_LIMIT) {
		return query(retrieve_rows_sql(*this, table, prev_key, last_key, row_count), row_packer);
	}

	size_t count_rows(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
		return atoi(select_one(count_rows_sql(*this, table, prev_key, last_key)).c_str());
	}

	void execute(const string &sql);
	void disable_referential_integrity();
	void enable_referential_integrity();
	string export_snapshot();
	void import_snapshot(const string &snapshot);
	void unhold_snapshot();
	void start_read_transaction();
	void start_write_transaction();
	void commit_transaction();
	void rollback_transaction();
	void populate_database_schema(Database &database);
	string escape_value(const string &value);
	string column_definition(const Column &column);

	inline char quote_identifiers_with() const { return '"'; }
	inline bool index_names_are_global() const { return true; }

protected:
	friend class PostgreSQLTableLister;

	template <typename RowFunction>
	size_t query(const string &sql, RowFunction &row_handler) {
		PostgreSQLRes res(PQexecParams(conn, sql.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0 /* text-format results only */));

		if (res.status() != PGRES_TUPLES_OK) {
			backtrace();
			throw runtime_error(PQerrorMessage(conn) + string("\n") + sql);
		}

		for (int row_number = 0; row_number < res.n_tuples(); row_number++) {
			PostgreSQLRow row(res, row_number);
			row_handler(row);
		}

		return res.n_tuples();
	}

	string select_one(const string &sql) {
		PostgreSQLRes res(PQexecParams(conn, sql.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0 /* text-format results only */));

		if (res.status() != PGRES_TUPLES_OK) {
			backtrace();
			throw runtime_error(PQerrorMessage(conn) + string("\n") + sql);
		}

		if (res.n_tuples() != 1 || res.n_columns() != 1) {
			throw runtime_error("Expected query to return only one row with only one column\n" + sql);
		}
		
		return PostgreSQLRow(res, 0).string_at(0);
	}

private:
	PGconn *conn;

	// forbid copying
	PostgreSQLClient(const PostgreSQLClient& copy_from) { throw logic_error("copying forbidden"); }
};

PostgreSQLClient::PostgreSQLClient(
	const string &database_host,
	const string &database_port,
	const string &database_name,
	const string &database_username,
	const string &database_password) {

	const char *keywords[] = { "host",                "port",                "dbname",              "user",                    "password",                nullptr };
	const char *values[]   = { database_host.c_str(), database_port.c_str(), database_name.c_str(), database_username.c_str(), database_password.c_str(), nullptr };

	conn = PQconnectdbParams(keywords, values, 1 /* allow expansion */);

	if (PQstatus(conn) != CONNECTION_OK) {
		throw runtime_error(PQerrorMessage(conn));
	}
	if (PQsetClientEncoding(conn, "SQL_ASCII")) {
		throw runtime_error(PQerrorMessage(conn));
	}
}

PostgreSQLClient::~PostgreSQLClient() {
	if (conn) {
		PQfinish(conn);
	}
}

void PostgreSQLClient::execute(const string &sql) {
    PostgreSQLRes res(PQexec(conn, sql.c_str()));

    if (res.status() != PGRES_COMMAND_OK) {
		throw runtime_error(PQerrorMessage(conn) + string("\n") + sql);
    }
}

void PostgreSQLClient::start_read_transaction() {
	execute("START TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ");
}

void PostgreSQLClient::start_write_transaction() {
	execute("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
}

void PostgreSQLClient::commit_transaction() {
	execute("COMMIT");
}

void PostgreSQLClient::rollback_transaction() {
	execute("ROLLBACK");
}

string PostgreSQLClient::export_snapshot() {
	// postgresql has transactional DDL, so by starting our transaction before we've even looked at the tables,
	// we'll get a 100% consistent view.
	execute("START TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ");
	return select_one("SELECT pg_export_snapshot()");
}

void PostgreSQLClient::import_snapshot(const string &snapshot) {
	execute("START TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ");
	execute("SET TRANSACTION SNAPSHOT '" + escape_value(snapshot) + "'");
}

void PostgreSQLClient::unhold_snapshot() {
	// do nothing - only needed for lock-based systems like mysql
}

void PostgreSQLClient::disable_referential_integrity() {
	execute("SET CONSTRAINTS ALL DEFERRED");

	/* TODO: investigate the pros and cons of disabling triggers - this blocks if there's a read transaction open
	for (const Table &table : database.tables) {
		execute("ALTER TABLE " + table.name + " DISABLE TRIGGER ALL");
	}
	*/
}

void PostgreSQLClient::enable_referential_integrity() {
	/* TODO: investigate the pros and cons of disabling triggers - this blocks if there's a read transaction open
	for (const Table &table : database.tables) {
		execute("ALTER TABLE " + table.name + " ENABLE TRIGGER ALL");
	}
	*/
}

string PostgreSQLClient::escape_value(const string &value) {
	string result;
	result.resize(value.size()*2 + 1);
	size_t result_length = PQescapeStringConn(conn, (char*)result.data(), value.c_str(), value.size(), nullptr);
	result.resize(result_length);
	return result;
}

string PostgreSQLClient::column_definition(const Column &column) {
	string result;
	result += quote_identifiers_with();
	result += column.name;
	result += quote_identifiers_with();
	result += ' ';

	if (column.column_type == ColumnTypes::BLOB) {
		result += "bytea";

	} else if (column.column_type == ColumnTypes::TEXT) {
		result += "text";

	} else if (column.column_type == ColumnTypes::VCHR) {
		result += "character varying(";
		result += to_string(column.size);
		result += ")";

	} else if (column.column_type == ColumnTypes::FCHR) {
		result += "character(";
		result += to_string(column.size);
		result += ")";

	} else if (column.column_type == ColumnTypes::BOOL) {
		result += "boolean";

	} else if (column.column_type == ColumnTypes::SINT || column.column_type == ColumnTypes::UINT) {
		switch (column.size) {
			case 1: // not used by postgresql; smallint is the nearest equivalent
			case 2:
				result += "smallint";
				break;

			case 3: // not used by postgresql; integer is the nearest equivalent
			case 4:
				result += "integer";
				break;

			default:
				result += "bigint";
		}

		// postgresql doesn't support unsigned columns; to make migration from databases that do
		// easier, we don't reject unsigned columns, we just convert them to the signed equivalent

	} else if (column.column_type == ColumnTypes::REAL) {
		result += (column.size == 4 ? "float" : "double precision");

	} else if (column.column_type == ColumnTypes::DECI) {
		result += "numeric(";
		result += to_string(column.size);
		result += ',';
		result += to_string(column.scale);
		result += ')';

	} else if (column.column_type == ColumnTypes::DATE) {
		result += "date without time zone";

	} else if (column.column_type == ColumnTypes::TIME) {
		result += "time without time zone";

	} else if (column.column_type == ColumnTypes::DTTM) {
		result += "datetime without time zone";

	} else {
		throw runtime_error("Don't know how to express postgresql column type of " + column.name + " (" + column.column_type + ")");
	}

	if (!column.nullable) {
		result += " NOT NULL";
	}

	if (column.default_set) {
		result += " DEFAULT ";
		result += escape_value(column.default_value);
	}

	return result;
}

struct PostgreSQLColumnLister {
	inline PostgreSQLColumnLister(Table &table): table(table) {}

	inline void operator()(PostgreSQLRow &row) {
		string name(row.string_at(0));
		string db_type(row.string_at(1));
		bool nullable(row.string_at(2) == "f");
		bool default_set(row.string_at(3) == "t");
		string default_value;

		if (default_set) {
			default_value = row.string_at(4);
			if (default_value.length() > 2 && default_value[0] == '\'') {
				default_value = unescape_value(default_value.substr(1, default_value.rfind('\'') - 1));
			}
		}

		if (db_type == "boolean") {
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::BOOL);
		} else if (db_type == "smallint") {
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::SINT, 2);
		} else if (db_type == "integer") {
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::SINT, 4);
		} else if (db_type == "bigint") {
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::SINT, 8);
		} else if (db_type == "real") {
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::REAL, 4);
		} else if (db_type == "double precision") {
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::REAL, 8);
		} else if (db_type.substr(0, 8) == "numeric(") {
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::DECI, extract_column_length(db_type), extract_column_scale(db_type));
		} else if (db_type.substr(0, 18) == "character varying(") {
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::VCHR, extract_column_length(db_type));
		} else if (db_type.substr(0, 10) == "character(") {
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::FCHR, extract_column_length(db_type));
		} else if (db_type == "text") {
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::TEXT);
		} else if (db_type == "bytea") {
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::BLOB);
		} else if (db_type == "date") {
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::DATE);
		} else if (db_type == "time without time zone") { // TODO: consider support for 'with time zone'
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::TIME);
		} else if (db_type == "timestamp without time zone") { // TODO: consider support for 'with time zone'
			table.columns.emplace_back(name, nullable, default_set, default_value, ColumnTypes::DTTM);
		} else {
			throw runtime_error("Don't know how to represent postgresql type of " + table.name + '.' + name + " (" + db_type + ")");
		}
	}

	inline string unescape_value(const string &escaped) {
		string result;
		result.reserve(escaped.length());
		for (string::size_type n = 0; n < escaped.length(); n++) {
			// this is by no means a complete unescaping function, it only handles the cases seen in
			// the output of pg_get_expr so far
			if (escaped[n] == '\\' || escaped[n] == '\'') {
				n += 1;
			}
			result += escaped[n];
		}
		return result;
	}

	Table &table;
};

struct PostgreSQLPrimaryKeyLister {
	inline PostgreSQLPrimaryKeyLister(Table &table): table(table) {}

	inline void operator()(PostgreSQLRow &row) {
		string column_name = row.string_at(0);
		size_t column_index = table.index_of_column(column_name);
		table.primary_key_columns.push_back(column_index);
	}

	Table &table;
};

struct PostgreSQLKeyLister {
	inline PostgreSQLKeyLister(Table &table): table(table) {}

	inline void operator()(PostgreSQLRow &row) {
		// if we have no primary key, we might need to use another unique key as a surrogate - see PostgreSQLTableLister below
		// furthermore this key must have no NULLable columns, as they effectively make the index not unique
		string key_name = row.string_at(0);
		bool unique = (row.string_at(1) == "t");
		string column_name = row.string_at(2);
		size_t column_index = table.index_of_column(column_name);
		// FUTURE: consider representing collation, index type, partial keys etc.

		if (table.keys.empty() || table.keys.back().name != key_name) {
			table.keys.push_back(Key(key_name, unique));
		}
		table.keys.back().columns.push_back(column_index);

		if (table.primary_key_columns.empty()) {
			// if we have no primary key, we might need to use another unique key as a surrogate - see MySQLTableLister below -
			// but this key must have no NULLable columns, as they effectively make the index not unique
			bool nullable = (row.string_at(3) == "f");
			if (unique && nullable) {
				// mark this as unusable
				unique_but_nullable_keys.insert(key_name);
			}
		}
	}

	Table &table;
	set<string> unique_but_nullable_keys;
};

struct PostgreSQLTableLister {
	PostgreSQLTableLister(PostgreSQLClient &client, Database &database): client(client), database(database) {}

	void operator()(PostgreSQLRow &row) {
		Table table(row.string_at(0));

		PostgreSQLColumnLister column_lister(table);
		client.query(
			"SELECT attname, format_type(atttypid, atttypmod), attnotnull, atthasdef, pg_get_expr(adbin, adrelid) "
			  "FROM pg_attribute "
			  "JOIN pg_class ON attrelid = pg_class.oid "
			  "JOIN pg_type ON atttypid = pg_type.oid "
			  "LEFT JOIN pg_attrdef ON adrelid = attrelid AND adnum = attnum "
			 "WHERE attnum > 0 AND "
			       "NOT attisdropped AND "
			       "relname = '" + table.name + "' "
			 "ORDER BY attnum",
			column_lister);

		PostgreSQLPrimaryKeyLister primary_key_lister(table);
		client.query(
			"SELECT column_name "
			  "FROM information_schema.table_constraints, "
			       "information_schema.key_column_usage "
			 "WHERE information_schema.table_constraints.table_name = '" + table.name + "' AND "
			       "information_schema.key_column_usage.table_name = information_schema.table_constraints.table_name AND "
			       "constraint_type = 'PRIMARY KEY' "
			 "ORDER BY ordinal_position",
			primary_key_lister);

		PostgreSQLKeyLister key_lister(table);
		client.query(
			"SELECT index_class.relname, pg_index.indisunique, attname, attnotnull "
			  "FROM pg_class table_class, pg_index, pg_class index_class, generate_subscripts(indkey, 1) AS position, pg_attribute "
			 "WHERE table_class.oid = pg_index.indrelid AND "
			       "pg_index.indexrelid = index_class.oid AND index_class.relkind = 'i' AND "
			       "table_class.oid = pg_attribute.attrelid AND pg_attribute.attnum = indkey[position] AND "
			       "table_class.relname = '" + table.name + "' AND "
			       "NOT pg_index.indisprimary "
			 "ORDER BY relname, position",
			key_lister);

		// if the table has no primary key, we need to find a unique key with no nullable columns to act as a surrogate primary key
		sort(table.keys.begin(), table.keys.end()); // order is arbitrary for keys, but both ends must be consistent, so we sort the keys by name
		
		for (Keys::const_iterator key = table.keys.begin(); key != table.keys.end() && table.primary_key_columns.empty(); ++key) {
			if (key->unique && !key_lister.unique_but_nullable_keys.count(key->name)) {
				table.primary_key_columns = key->columns;
			}
		}
		if (table.primary_key_columns.empty()) {
			// of course this falls apart if there are no unique keys, so we don't allow that
			throw runtime_error("Couldn't find a primary or non-nullable unique key on table " + table.name);
		}

		database.tables.push_back(table);
	}

	PostgreSQLClient &client;
	Database &database;
};

void PostgreSQLClient::populate_database_schema(Database &database) {
	PostgreSQLTableLister table_lister(*this, database);
	query("SELECT tablename "
		    "FROM pg_tables "
		   "WHERE schemaname = ANY (current_schemas(false)) "
		   "ORDER BY pg_relation_size(tablename::text) DESC, tablename ASC",
		  table_lister);
}


int main(int argc, char *argv[]) {
	return endpoint_main<PostgreSQLClient>(argc, argv);
}
