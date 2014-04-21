#ifndef SQL_FUNCTIONS_H
#define SQL_FUNCTIONS_H

#include <string>
#include <vector>

#include "schema.h"

using namespace std;

template <typename DatabaseClient>
string columns_list(DatabaseClient &client, const Columns &columns, const ColumnIndices &column_indices) {
	if (column_indices.empty()) {
		return "(NULL)";
	}

	string result("(");
	result += columns[*column_indices.begin()].name;
	for (ColumnIndices::const_iterator column_index = column_indices.begin() + 1; column_index != column_indices.end(); ++column_index) {
		result += ", ";
		if (client.quote_identifiers_with()) result += client.quote_identifiers_with();
		result += columns[*column_index].name;
		if (client.quote_identifiers_with()) result += client.quote_identifiers_with();
	}
	result += ")";
	return result;
}

template <typename DatabaseClient>
string non_binary_string_values_list(DatabaseClient &client, const vector<string> &values) {
	if (values.empty()) {
		return "(NULL)";
	}

	string result("('");
	result.append(*values.begin());
	for (vector<string>::const_iterator value = values.begin() + 1; value != values.end(); ++value) {
		result.append("', '");
		result.append(client.escape_value(*value));
	}
	result.append("')");
	return result;
}

template <typename DatabaseClient>
string where_sql(DatabaseClient &client, const string &key_columns, const ColumnValues &prev_key, const ColumnValues &last_key, const string &extra_where_conditions = "", const char *prefix = " WHERE ") {
	string result;
	if (!prev_key.empty()) {
		result += prefix;
		result += key_columns;
		result += " > ";
		result += non_binary_string_values_list(client, prev_key);
		prefix = " AND ";
	}
	if (!last_key.empty()) {
		result += prefix;
		result += key_columns;
		result += " <= ";
		result += non_binary_string_values_list(client, last_key);
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
		if (client.quote_identifiers_with()) result += client.quote_identifiers_with();
		result += column->name;
		if (client.quote_identifiers_with()) result += client.quote_identifiers_with();
	}
	return result;
}

template <typename DatabaseClient>
string retrieve_rows_sql(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, size_t row_count) {
	string key_columns(columns_list(client, table.columns, table.primary_key_columns));

	string result("SELECT ");
	result += select_columns_sql(client, table);
	result += " FROM ";
	result += table.name;
	result += where_sql(client, key_columns, prev_key, ColumnValues(), table.where_conditions);
	result += " ORDER BY " + key_columns.substr(1, key_columns.size() - 2);
	result += " LIMIT " + to_string(row_count);
	return result;
}

template <typename DatabaseClient>
string retrieve_rows_sql(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
	string key_columns(columns_list(client, table.columns, table.primary_key_columns));

	string result("SELECT ");
	result += select_columns_sql(client, table);
	result += " FROM ";
	result += table.name;
	result += where_sql(client, key_columns, prev_key, last_key, table.where_conditions);
	result += + " ORDER BY " + key_columns.substr(1, key_columns.size() - 2);
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

#endif
