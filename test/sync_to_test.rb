require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SyncToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  def setup
    expect_handshake_commands
  end

  def setup_with_footbl
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str'), (101, 0, NULL), (1000, 0, NULL), (1001, 0, 'last')"
    @rows = [["2",    "10",       "test"],
             ["4",     nil,        "foo"],
             ["5",     nil,          nil],
             ["8",    "-1", "longer str"],
             ["101",   "0",          nil],
             ["1000",  "0",          nil],
             ["1001",  "0",       "last"]]
    @keys = @rows.collect {|row| [row[0]]}
  end

  test_each "accepts being immediately sent all rows if the other end has an empty table, and finishes without needing to make any changes if the table is empty" do
    clear_schema
    create_footbl

    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:open).with("footbl").
      returns([[Commands::ROWS_NEXT, []], []])
    expects(:quit)
    receive_commands

    assert_equal [],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "accepts being immediately sent all rows if the other end has an empty table, and clears the table if it is not empty" do
    clear_schema
    setup_with_footbl

    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:open).with("footbl").
      returns([[Commands::ROWS_NEXT, []], []])
    expects(:quit)
    receive_commands

    assert_equal [],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "accepts matching hashes and asked for the hash of the next row(s), doubling the number of rows" do
    setup_with_footbl

    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:open).with("footbl").
      returns([[Commands::HASH_NEXT, @keys[0], hash_of(@rows[0..0])]])
    expects(:hash_next).with(@keys[2], hash_of(@rows[1..2])).
      returns([[Commands::HASH_NEXT, @keys[6], hash_of(@rows[3..6])]])
    expects(:rows_next).with([]).
      returns([[Commands::ROWS_CURR, []], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "requests and applies the row if we send a different hash for a single row, and then carries on with the hash sent after that" do
    setup_with_footbl
    execute "UPDATE footbl SET col3 = 'different' WHERE col1 = 2"

    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:open).with("footbl").
      returns([[Commands::HASH_NEXT, @keys[0], hash_of(@rows[0..0])]])
    expects(:rows_curr).with(@keys[0]).
      returns([[Commands::ROWS_CURR, @keys[0]], @rows[0], [],
               [Commands::HASH_NEXT, @keys[1], hash_of(@rows[1..1])]])
    expects(:hash_next).with(@keys[3], hash_of(@rows[2..3])).
      returns([[Commands::HASH_NEXT, @keys[-1], hash_of(@rows[4..-1])]])
    expects(:rows_next).with([]).
      returns([[Commands::ROWS_CURR, []], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "reduces the search range and tries again if we send a different hash for multiple rows" do
    setup_with_footbl
    execute "UPDATE footbl SET col3 = 'different' WHERE col1 = 101"

    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:open).with("footbl").
      returns([[Commands::HASH_NEXT, @keys[0], hash_of(@rows[0..0])]])
    expects(:hash_next).with(@keys[2], hash_of(@rows[1..2])).
      returns([[Commands::HASH_NEXT, @keys[6], hash_of(@rows[3..6])]])
    expects(:hash_curr).with(@keys[4], hash_of([@rows[3], ["101", "0", "different"]])).
      returns([[Commands::HASH_CURR, @keys[4], hash_of(@rows[3..4])]])
    expects(:hash_curr).with(@keys[3], hash_of(@rows[3..3])).
      returns([[Commands::HASH_NEXT, @keys[5], hash_of(@rows[4..5])]]) # (not ideal that it's doubled the row count again, since really we know by now @rows[4] is the issue)
    expects(:hash_curr).with(@keys[4], hash_of([["101", "0", "different"]])).
      returns([[Commands::ROWS_CURR, @keys[4]], @rows[4], [],
               [Commands::HASH_NEXT, @keys[5], hash_of(@rows[5..5])]])
    expects(:hash_next).with(@keys[6], hash_of(@rows[6..6])).
      returns([[Commands::ROWS_NEXT, []], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "handles data after nil elements" do
    clear_schema
    create_footbl
    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:open).with("footbl").
      returns([[Commands::ROWS_CURR, []], ["2", nil, nil], ["3",  nil,  "foo"], []])
    expects(:quit)
    receive_commands

    assert_equal [["2", nil,   nil],
                  ["3", nil, "foo"]],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "handles hashing medium values" do
    clear_schema
    create_texttbl

    execute "INSERT INTO texttbl VALUES (0, 'test'), (1, '#{'a'*16*1024}')"

    @rows = [["0", "test"],
             ["1", "a"*16*1024]]
    @keys = @rows.collect {|row| [row[0]]}

    expects(:schema).with().
      returns([{"tables" => [texttbl_def]}])
    expects(:open).with("texttbl").
      returns([[Commands::HASH_NEXT, @keys[0], hash_of(@rows[0..0])]])
    expects(:hash_next).in_sequence.with(@keys[1], hash_of(@rows[1..1])).
      returns([[Commands::ROWS_NEXT, []], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles requesting and saving medium values" do
    clear_schema
    create_texttbl

    @rows = [["1", "a"*16*1024]]
    @keys = @rows.collect {|row| [row[0]]}

    expects(:schema).with().
      returns([{"tables" => [texttbl_def]}])
    expects(:open).with("texttbl").
      returns([[Commands::HASH_NEXT, @keys[0], hash_of(@rows[0..0])]])
    expects(:rows_curr).in_sequence.with([]).
      returns([[Commands::ROWS_CURR, []], @rows[0], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles hashing long values" do
    clear_schema
    create_texttbl
    execute "INSERT INTO texttbl VALUES (0, 'test'), (1, '#{'a'*80*1024}')"

    @rows = [["0", "test"],
             ["1", "a"*80*1024]]
    @keys = @rows.collect {|row| [row[0]]}

    expects(:schema).with().
      returns([{"tables" => [texttbl_def]}])
    expects(:open).with("texttbl").
      returns([[Commands::HASH_NEXT, @keys[0], hash_of(@rows[0..0])]])
    expects(:hash_next).in_sequence.with(@keys[1], hash_of(@rows[1..1])).
      returns([[Commands::ROWS_NEXT, []], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles requesting and saving long values" do
    clear_schema
    create_texttbl

    @rows = [["1", "a"*80*1024]]
    @keys = @rows.collect {|row| [row[0]]}

    expects(:schema).with().
      returns([{"tables" => [texttbl_def]}])
    expects(:open).with("texttbl").
      returns([[Commands::HASH_NEXT, @keys[0], hash_of(@rows[0..0])]])
    expects(:rows_curr).in_sequence.with([]).
      returns([[Commands::ROWS_CURR, []], @rows[0], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles reusing unique values that were previously on later rows" do
    setup_with_footbl
    execute "CREATE UNIQUE INDEX unique_key ON footbl (col3)"

    @orig_rows = @rows.collect {|row| row.dup}
    @rows[0][-1] = @rows[-1][-1] # reuse this value from the last row
    @rows[-1][-1] = "new value"  # and change it there to something else

    expects(:schema).with().
      returns([{"tables" => [footbl_def.merge("keys" => [{"name" => "unique_key", "unique" => true, "columns" => [2]}])]}])
    expects(:open).with("footbl").
      returns([[Commands::HASH_NEXT, @keys[0], hash_of(@rows[0..0])]])
    expects(:rows_curr).with(@keys[0]).
      returns([[Commands::ROWS_CURR, @keys[0]], @rows[0], [],
               [Commands::HASH_NEXT, @keys[1], hash_of(@rows[1..1])]])
    expects(:hash_next).with(@keys[3], hash_of(@rows[2..3])).
      returns([[Commands::HASH_NEXT, @keys[6], hash_of(@rows[4..6])]])
    expects(:hash_curr).with(@keys[4], hash_of(@orig_rows[4..4])).
      returns([[Commands::HASH_NEXT, @keys[6], hash_of(@orig_rows[5..6])]])
    expects(:rows_next).with([]).
      returns([[Commands::ROWS_CURR, []], @rows[6], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end
end
