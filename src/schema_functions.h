#ifndef SCHEMA_FUNCTIONS_H
#define SCHEMA_FUNCTIONS_H

#include <stdexcept>
#include <set>
#include <list>
#include <algorithm>

#include <iostream>

#include "schema.h"
#include "sql_functions.h"

inline int extract_column_length(const string &db_type) {
       size_t pos = db_type.find('(');
       if (pos >= db_type.length() - 1) throw runtime_error("Couldn't find length in type specification " + db_type);
       return atoi(db_type.c_str() + pos + 1);
}

inline int extract_column_scale(const string &db_type) {
       size_t pos = db_type.find(',');
       if (pos >= db_type.length() - 1) throw runtime_error("Couldn't find scale in type specification " + db_type);
       return atoi(db_type.c_str() + pos + 1);
}

string unquoted_column_names_list(const Columns &columns, const ColumnIndices &column_indices);

template <typename T>
struct name_is {
	const string &name;
	name_is(const string &name): name(name) {}
	bool operator()(const T& obj) const {
		return (obj.name == name);
	}
};

struct key_column_matches {
	const Columns &columns1;
	const Columns &columns2;
	key_column_matches(const Columns &columns1, const Columns &columns2): columns1(columns1), columns2(columns2) {}
	bool operator()(size_t index1, size_t index2) const {
		return (columns1[index1] == columns2[index2]);
	}
};

struct schema_mismatch: public runtime_error {
	schema_mismatch(const string &error): runtime_error(error) { }
};

template <typename DatabaseClient>
struct SchemaMatcher {
	SchemaMatcher(DatabaseClient &client): client(client) {}

	void match_schemas(const Database &from_database, const Database &to_database) {
		// currently we only pay attention to tables, but in the future we might support other schema items
		match_tables(from_database.tables, to_database.tables);
	}

protected:
	void match_tables(Tables from_tables, Tables to_tables) { // copies arguments
		// sort the table lists so they have the same order
		sort(from_tables.begin(), from_tables.end());
		sort(  to_tables.begin(),   to_tables.end());

		Tables::iterator from_table = from_tables.begin();
		Tables::iterator   to_table =   to_tables.begin();
		while (to_table != to_tables.end()) {
			if (from_table == from_tables.end() ||
				from_table->name > to_table->name) {
				// our end has an extra table, drop it
				queue_drop_table(to_table->name);
				to_table = to_tables.erase(to_table);
				// keep the current from_table and re-evaluate on the next iteration

			} else if (to_table->name > from_table->name) {
				queue_create_table(*from_table);
				to_table = ++to_tables.insert(to_table, *from_table);
				++from_table;

			} else if (must_recreate_table(*from_table, *to_table)) {
				queue_drop_table(to_table->name);
				queue_create_table(*from_table);
				*to_table = *from_table;
				++to_table;
				++from_table;

			} else {
				match_table(*from_table, *to_table);
				++to_table;
				++from_table;
			}
		}
		while (from_table != from_tables.end()) {
			queue_create_table(*from_table);
			to_tables.push_back(*from_table);
			++from_table;
		}
	}

	bool must_recreate_table(const Table &from_table, const Table &to_table) {
		// if any of the primary key columns have changed, we recreate the table, because different
		// database servers behave quite differently when we try to alter the primary key columns;
		// since it is very rare for PKs to change, it isn't worth the complexity that would be
		// introduced if we tried to fix things up in-place and predict/remedy all those cases.
		return !primary_key_matches(from_table, to_table);
	}

	bool primary_key_matches(const Table &from_table, const Table &to_table) {
		return (from_table.primary_key_columns.size() == to_table.primary_key_columns.size() &&
				mismatch(from_table.primary_key_columns.begin(),
						 from_table.primary_key_columns.end(),
						 to_table.primary_key_columns.begin(),
						 key_column_matches(from_table.columns, to_table.columns)).
					first == from_table.primary_key_columns.end());
	}

	void match_table(Table &from_table, Table &to_table) {
		check_columns_match(from_table, from_table.columns, to_table.columns);
		match_keys(from_table, from_table.keys, to_table.keys);
		// FUTURE: check collation etc.
	}


	void match_keys(const Table &table, Keys &from_keys, Keys &to_keys) {
		// sort the key lists so they have the same order
		sort(from_keys.begin(), from_keys.end());
		sort(  to_keys.begin(),   to_keys.end());

		Keys::const_iterator from_key = from_keys.begin();
		Keys::iterator         to_key =   to_keys.begin();
		while (to_key != to_keys.end()) {
			if (from_key == from_keys.end() ||
				from_key->name > to_key->name) {
				// our end has an extra key, drop it
				queue_drop_key(table, *to_key);
				to_key = to_keys.erase(to_key);
				// keep the current from_key and re-evaluate on the next iteration

			} else if (to_key->name > from_key->name) {
				// their end has an extra key, add it
				queue_add_key(table, *from_key);
				to_key = ++to_keys.insert(to_key, *from_key);
				++from_key;
				// keep the current to_key and re-evaluate on the next iteration

			} else {
				check_key_match(table, *from_key, *to_key);
				++to_key;
				++from_key;
			}
		}
		while (from_key != from_keys.end()) {
			queue_add_key(table, *from_key);
			to_key = ++to_keys.insert(to_key, *from_key);
			++from_key;
		}
	}

	void check_key_match(const Table &table, const Key &from_key, const Key &to_key) {
		if (from_key.unique != to_key.unique ||
			from_key.columns != to_key.columns) {
			// recreate the index.  not all databases can combine these two statements, so we implement the general case only for now.
			queue_drop_key(table, to_key);
			queue_add_key(table, from_key);
		}
	}


	void check_columns_match(const Table &table, const Columns &from_columns, Columns &to_columns) {
		Columns columns_to_drop;
		Columns::const_iterator from_column = from_columns.begin();
		Columns::iterator         to_column =   to_columns.begin();
		while (to_column != to_columns.end()) {
			if (from_column != from_columns.end() &&
				from_column->name == to_column->name) {
				check_column_match(table, *from_column, *to_column);
				++to_column;
				++from_column;

			} else if (find_if(from_column, from_columns.end(), name_is<Column>(to_column->name)) == from_columns.end()) {
				// our end has an extra column, drop it
				columns_to_drop.push_back(*to_column);
				to_column = to_columns.erase(to_column);
				// keep the current from_column and re-evaluate on the next iteration

			} else if (find_if(to_column, to_columns.end(), name_is<Column>(from_column->name)) == to_columns.end()) {
				throw schema_mismatch("Missing column " + from_column->name + " on table " + table.name);

			} else {
				throw schema_mismatch("Misordered column " + from_column->name + " on table " + table.name + ", should have " + to_column->name + " first");
			}
		}
		if (from_column != from_columns.end()) {
			throw schema_mismatch("Missing column " + from_column->name + " on table " + table.name);
		}
		if (!columns_to_drop.empty()) {
			queue_drop_columns(table, columns_to_drop);
		}
	}

	void check_column_match(const Table &table, const Column &from_column, const Column &to_column) {
		// FUTURE: check collation etc.
		if (from_column.column_type != to_column.column_type) {
			throw schema_mismatch("Column " + from_column.name + " on table " + table.name +
				" should have type " + from_column.column_type +
				" but has type " + to_column.column_type);
		}
		if (from_column.size != to_column.size) {
			throw schema_mismatch("Column " + from_column.name + " on table " + table.name +
				" should have size " + to_string(from_column.size) +
				" but has size " + to_string(to_column.size));
		}
		if (from_column.nullable != to_column.nullable) {
			throw schema_mismatch("Column " + from_column.name + " on table " + table.name +
				" should be " + (from_column.nullable ? "nullable" : "not nullable") +
				" but is " + (to_column.nullable ? "nullable" : "not nullable"));
		}
		if (from_column.default_set != to_column.default_set ||
			(from_column.default_set && (from_column.default_value != to_column.default_value))) {
			throw schema_mismatch("Column " + from_column.name + " on table " + table.name +
				" should " + (from_column.default_set ? "have default " + from_column.default_value : "not have default") +
				" but " + (to_column.default_set ? "has default " + to_column.default_value : "doesn't have default"));
		}
	}

protected:
	void queue_create_table(const Table &table) {
		statements.push_back(create_table_sql(client, table));

		for (const Key &key : table.keys) {
			statements.push_back(add_key_sql(client, table, key));
		}
	}

	void queue_drop_table(const Table &table) {
		statements.push_back(drop_table_sql(client, table));
	}

	void queue_add_key(const Table &table, const Key &key) {
		statements.push_back(add_key_sql(client, table, key));
	}

	void queue_drop_key(const Table &table, const Key &key) {
		statements.push_back(drop_key_sql(client, table, key));
	}

	void queue_drop_columns(const Table &table, const Columns &columns) {
		statements.push_back(drop_columns_sql(client, table, columns));
	}

protected:
	DatabaseClient &client;
	list<string> statements;
};

#endif
