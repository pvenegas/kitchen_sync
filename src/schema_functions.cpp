#include "schema_functions.h"

string unquoted_column_names_list(const Columns &columns, const ColumnIndices &column_indices) {
	if (column_indices.empty()) {
		return "(NULL)";
	}

	string result("(");
	result.append(columns[*column_indices.begin()].name);
	for (ColumnIndices::const_iterator column_index = column_indices.begin() + 1; column_index != column_indices.end(); ++column_index) {
		result.append(", ");
		result.append(columns[*column_index].name);
	}
	result.append(")");
	return result;
}
