#ifndef SQL_FUNCTIONS_H
#define SQL_FUNCTIONS_H

#include <string>
#include <vector>

#include "schema.h"
#include "encode_packed.h"

using namespace std;

template <typename DatabaseClient>
string columns_list(DatabaseClient &client, const Columns &columns, const ColumnIndices &column_indices) {
	if (column_indices.empty()) {
		return "(NULL)";
	}

	string result("(");
	result += client.quote_identifiers_with();
	result += columns[*column_indices.begin()].name;
	result += client.quote_identifiers_with();
	for (ColumnIndices::const_iterator column_index = column_indices.begin() + 1; column_index != column_indices.end(); ++column_index) {
		result += ", ";
		result += client.quote_identifiers_with();
		result += columns[*column_index].name;
		result += client.quote_identifiers_with();
	}
	result += ")";
	return result;
}

template <typename DatabaseClient>
string values_list(DatabaseClient &client, const ColumnValues &values) {
	if (values.empty()) {
		return "(NULL)";
	}

	string result("(");
	result += encode(client, values.front());
	for (ColumnValues::const_iterator value = values.begin() + 1; value != values.end(); ++value) {
		result += ',';
		result += encode(client, *value);
	}
	result += ")";
	return result;
}

template <typename DatabaseClient>
string where_sql(DatabaseClient &client, const string &key_columns, const ColumnValues &prev_key, const ColumnValues &last_key, const string &extra_where_conditions = "", const char *prefix = " WHERE ") {
	string result;
	if (!prev_key.empty()) {
		result += prefix;
		result += key_columns;
		result += " > ";
		result += values_list(client, prev_key);
		prefix = " AND ";
	}
	if (!last_key.empty()) {
		result += prefix;
		result += key_columns;
		result += " <= ";
		result += values_list(client, last_key);
		prefix = " AND ";
	}
	if (!extra_where_conditions.empty()) {
		result += prefix;
		result += extra_where_conditions;
	}
	return result;
}

template <typename DatabaseClient>
string select_columns_sql(DatabaseClient &client, const Table &table) {
	string result;
	for (Columns::const_iterator column = table.columns.begin(); column != table.columns.end(); ++column) {
		if (column != table.columns.begin()) result += ", ";
		if (!column->filter_expression.empty()) {
			result += column->filter_expression;
			result += " AS ";
		}
		result += client.quote_identifiers_with();
		result += column->name;
		result += client.quote_identifiers_with();
	}
	return result;
}

const ssize_t NO_ROW_COUNT_LIMIT = -1;

template <typename DatabaseClient>
string retrieve_rows_sql(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, ssize_t row_count = NO_ROW_COUNT_LIMIT) {
	string key_columns(columns_list(client, table.columns, table.primary_key_columns));

	string result("SELECT ");
	result += select_columns_sql(client, table);
	result += " FROM ";
	result += table.name;
	result += where_sql(client, key_columns, prev_key, last_key, table.where_conditions);
	result += " ORDER BY " + key_columns.substr(1, key_columns.size() - 2);
	if (row_count != NO_ROW_COUNT_LIMIT) {
		result += " LIMIT " + to_string(row_count);
	}
	return result;
}

template <typename DatabaseClient>
string count_rows_sql(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
	string key_columns(columns_list(client, table.columns, table.primary_key_columns));

	string result("SELECT COUNT(*) FROM ");
	result += table.name;
	result += where_sql(client, key_columns, prev_key, last_key, table.where_conditions);
	return result;
}

template <typename DatabaseClient>
string drop_table_sql(DatabaseClient &client, const Table &table) {
	return "DROP TABLE " + table.name;
}

template <typename DatabaseClient>
string drop_key_sql(DatabaseClient &client, const Table &table, const Key &key) {
	string result;
	if (!client.index_names_are_global()) {
		result += "ALTER TABLE ";
		result += table.name;
		result += ' ';
	}
	result += "DROP INDEX ";
	result += client.quote_identifiers_with();
	result += key.name;
	result += client.quote_identifiers_with();
	return result;
}

template <typename DatabaseClient>
string add_key_sql(DatabaseClient &client, const Table &table, const Key &key) {
	string result;
	result += key.unique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ";
	result += client.quote_identifiers_with();
	result += key.name;
	result += client.quote_identifiers_with();
	result += " ON ";
	result += table.name;
	result += ' ';
	result += columns_list(client, table.columns, key.columns);
	return result;
}

template <typename DatabaseClient>
string drop_columns_sql(DatabaseClient &client, const Table &table, const Columns &columns) {
	string result("ALTER TABLE ");
	result += table.name;
	for (Columns::const_iterator column = columns.begin(); column != columns.end(); ++column) {
		result += (column == columns.begin() ? " DROP COLUMN " : ", DROP COLUMN ");
		result += client.quote_identifiers_with();
		result += column->name;
		result += client.quote_identifiers_with();
	}
	return result;
}

#endif
