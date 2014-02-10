#include "command.h"
#include "schema_serialization.h"
#include "sync_algorithm.h"
#include "fdstream.h"

template<class DatabaseClient>
struct SyncFromWorker {
	SyncFromWorker(const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password, int read_from_descriptor, int write_to_descriptor):
		client(database_host, database_port, database_name, database_username, database_password),
		in(read_from_descriptor),
		input(in),
		out(write_to_descriptor),
		output(out),
		row_packer(output) {
	}

	void operator()() {
		negotiate_protocol_version();

		string current_table_name;
		ColumnValues prev_key;
		ColumnValues last_key;

		try {
			Command command;

			while (true) {
				input >> command;

				if (command.verb == Commands::OPEN) {
					current_table_name = command.argument<string>(0);
					handle_open_command(current_table_name, prev_key, last_key);

				} else if (command.verb == Commands::HASH_CURR) {
					last_key = command.argument<ColumnValues>(0);
					string hash = command.argument<string>(1);
					handle_hash_command(current_table_name, prev_key, last_key, hash);

				} else if (command.verb == Commands::HASH_NEXT) {
					prev_key = last_key;
					last_key = command.argument<ColumnValues>(0);
					string hash = command.argument<string>(1);
					handle_hash_command(current_table_name, prev_key, last_key, hash);

				} else if (command.verb == Commands::ROWS_CURR) {
					last_key = command.argument<ColumnValues>(0);
					handle_rows_command(current_table_name, prev_key, last_key);

				} else if (command.verb == Commands::ROWS_NEXT) {
					prev_key = last_key;
					last_key = command.argument<ColumnValues>(0);
					handle_rows_command(current_table_name, prev_key, last_key);

				} else if (command.verb == Commands::EXPORT_SNAPSHOT) {
					output << client.export_snapshot();

				} else if (command.verb == Commands::IMPORT_SNAPSHOT) {
					string snapshot(command.argument<string>(0));
					client.import_snapshot(snapshot);
					output.pack_nil(); // arbitrary, sent to indicate we've started our transaction

				} else if (command.verb == Commands::UNHOLD_SNAPSHOT) {
					client.unhold_snapshot();
					output.pack_nil(); // similarly arbitrary

				} else if (command.verb == Commands::WITHOUT_SNAPSHOT) {
					client.start_read_transaction();
					output.pack_nil(); // similarly arbitrary

				} else if (command.verb == Commands::SCHEMA) {
					output << client.database_schema();

				} else if (command.verb == Commands::QUIT) {
					break;

				} else {
					throw command_error("Unknown command " + to_string(command.verb));
				}

				output.flush();
			}
		} catch (const exception &e) {
			// in fact we just output these errors much the same way that our caller does, but we do it here (before the stream gets closed) to help tests
			cerr << e.what() << endl;
			throw sync_error();
		}
	}

	void negotiate_protocol_version() {
		const int PROTOCOL_VERSION_SUPPORTED = 1;

		// all conversations must start with a Commands::PROTOCOL command to establish the language to be used
		Command command;
		input >> command;
		if (command.verb != Commands::PROTOCOL) {
			throw command_error("Expected a protocol command before " + to_string(command.verb));
		}

		// the usable protocol is the highest out of those supported by the two ends
		protocol = min(PROTOCOL_VERSION_SUPPORTED, (int)command.argument<int64_t>(0));

		// tell the other end what version was selected
		output << protocol;
		output.flush();
	}

	inline void send_hash_command(const Table &table, verb_t verb, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash) {
		// tell the other end to check its hash of the same rows, using key ranges rather than a count to improve the chances of a match.
		send_command(output, verb, last_key, hash);
	}

	inline void send_rows_response(const Table &table, verb_t verb, const ColumnValues &prev_key, const ColumnValues &last_key) {
		send_command(output, verb, last_key);
		client.retrieve_rows(table, prev_key, last_key, row_packer);
		row_packer.pack_end();
	}

	inline void send_rows_command(const Table &table, verb_t verb, ColumnValues &prev_key, ColumnValues &last_key) {
		// rows don't match, and there's only one or no rows in the range, so send it straight across, as if they had given the rows command
		send_rows_response(table, verb, prev_key, last_key);

		// if that range extended to the end of the table, we're done
		if (last_key.empty()) return;

		// otherwise follow up straight away with the next command
		prev_key = last_key;
		find_hash_of_next_range(*this, client, table, 1, prev_key, last_key, Commands::HASH_NEXT);
	}

	void handle_open_command(const string &table_name, ColumnValues &prev_key, ColumnValues &last_key) {
		const Table &table(client.table_by_name(table_name));

		prev_key = ColumnValues();
		find_hash_of_next_range(*this, client, table, 1, prev_key, last_key, Commands::HASH_NEXT);
	}

	void handle_hash_command(const string &table_name, ColumnValues &prev_key, ColumnValues &last_key, string &hash) {
		const Table &table(client.table_by_name(table_name));

		check_hash_and_choose_next_range(*this, client, table, prev_key, last_key, hash);
	}

	void handle_rows_command(const string &table_name, ColumnValues &prev_key, ColumnValues &last_key) {
		const Table &table(client.table_by_name(table_name));

		send_rows_command(table, Commands::ROWS_CURR, prev_key, last_key);
	}

	DatabaseClient client;
	FDReadStream in;
	Unpacker<FDReadStream> input;
	FDWriteStream out;
	Packer<FDWriteStream> output;
	RowPacker<typename DatabaseClient::RowType, FDWriteStream> row_packer;

	int protocol;
};

template<class DatabaseClient>
void sync_from(const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password, int read_from_descriptor, int write_to_descriptor) {
	SyncFromWorker<DatabaseClient> worker(database_host, database_port, database_name, database_username, database_password, read_from_descriptor, write_to_descriptor);
	worker();
}
