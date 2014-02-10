require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class RowsFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  test_each "returns all the rows in the key range greater than the previous key cursor and not greater than the given last key" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @rows = [["2",  "10",       "test"],
             ["4",   nil,        "foo"],
             ["5",   nil,          nil],
             ["8",  "-1", "longer str"]]
    @keys = @rows.collect {|row| [row[0]]}
    send_handshake_commands

    assert_equal [Commands::HASH_NEXT, ["2"], hash_of(@rows[0..0])],
     send_command(Commands::OPEN, "footbl")
    assert_equal [Commands::ROWS_CURR, ["2"]],
     send_command(Commands::ROWS_CURR, ["2"])
    assert_equal @rows[0], unpack_next
    assert_equal [], unpack_next
    assert_equal Commands::HASH_NEXT, unpack_next.first

    assert_equal [Commands::HASH_NEXT, ["2"], hash_of(@rows[0..0])],
     send_command(Commands::OPEN, "footbl")
    assert_equal [Commands::ROWS_CURR, ["1"]],
     send_command(Commands::ROWS_CURR, ["1"])
    assert_equal [], unpack_next
    assert_equal Commands::HASH_NEXT, unpack_next.first

    assert_equal [Commands::HASH_NEXT, ["2"], hash_of(@rows[0..0])],
     send_command(Commands::OPEN, "footbl")
    assert_equal [Commands::ROWS_CURR, ["3"]], # same range we requested, not the key of the last row they had
     send_command(Commands::ROWS_CURR, ["3"]) # different request, but same data matched
    assert_equal @rows[0], unpack_next
    assert_equal [], unpack_next
    assert_equal Commands::HASH_NEXT, unpack_next.first

    assert_equal [Commands::HASH_NEXT, ["2"], hash_of(@rows[0..0])],
     send_command(Commands::OPEN, "footbl")
    assert_equal [Commands::ROWS_CURR, ["4"]], # it's CURR because our NEXT moves the prev_key on
     send_command(Commands::ROWS_NEXT, ["4"]) # null numbers
    assert_equal @rows[1], unpack_next
    assert_equal [], unpack_next
    assert_equal Commands::HASH_NEXT, unpack_next.first

    assert_equal [Commands::ROWS_CURR, ["5"]],
     send_command(Commands::ROWS_CURR, ["5"]) # null strings
    assert_equal @rows[2], unpack_next
    assert_equal [], unpack_next
    assert_equal Commands::HASH_NEXT, unpack_next.first

    assert_equal [Commands::ROWS_CURR, ["9"]],
     send_command(Commands::ROWS_CURR, ["9"]) # negative numbers
    assert_equal @rows[3], unpack_next
    assert_equal [], unpack_next
    assert_equal [Commands::ROWS_NEXT, []],
      unpack_next
    assert_equal [], unpack_next

    assert_equal [Commands::HASH_NEXT, ["2"], hash_of(@rows[0..0])],
     send_command(Commands::OPEN, "footbl")
    assert_equal [Commands::ROWS_CURR, ["10"]],
     send_command(Commands::ROWS_CURR, ["10"])
    assert_equal @rows.shift, unpack_next
    assert_equal @rows.shift, unpack_next
    assert_equal @rows.shift, unpack_next
    assert_equal @rows.shift, unpack_next
    assert_equal [], unpack_next
    assert_equal [Commands::ROWS_NEXT, []],
      unpack_next
    assert_equal [], unpack_next
  end

  test_each "supports composite keys" do
    create_some_tables
    execute "INSERT INTO secondtbl VALUES (2349174, 'xy', 1, 2), (968116383, 'aa', 9, 9), (100, 'aa', 100, 100), (363401169, 'ab', 20, 340)"
    send_handshake_commands

    # note when reading these that the primary key columns are in reverse order to the table definition; the command arguments need to be given in the key order, but the column order for the results is unrelated

    assert_equal [Commands::HASH_NEXT, ["aa", "100"], hash_of([["100", "aa", "100", "100"]])],
     send_command(Commands::OPEN, "secondtbl")

    assert_equal [Commands::ROWS_CURR, ["aa", "10"]],
     send_command(Commands::ROWS_CURR, ["aa", "10"])
    assert_equal [], unpack_next
    assert_equal [Commands::HASH_NEXT, ["aa", "100"], hash_of([["100", "aa", "100", "100"]])],
      unpack_next

    assert_equal [Commands::ROWS_CURR, ["zz", "2147483647"]],
     send_command(Commands::ROWS_CURR, ["zz", "2147483647"])
    assert_equal [      "100", "aa", "100", "100"], unpack_next # first because aa is the first term in the key, then 100 the next
    assert_equal ["968116383", "aa",   "9",   "9"], unpack_next
    assert_equal ["363401169", "ab",  "20", "340"], unpack_next
    assert_equal [  "2349174", "xy",   "1",   "2"], unpack_next
    assert_equal [], unpack_next
    assert_equal [Commands::ROWS_NEXT, []],
      unpack_next
    assert_equal [], unpack_next

    assert_equal [Commands::HASH_NEXT, ["aa", "100"], hash_of([["100", "aa", "100", "100"]])],
     send_command(Commands::OPEN, "secondtbl")

    assert_equal [Commands::ROWS_CURR, ["aa", "101"]], # again, NEXT advances the cursor itself, so the old last key is now the start key
     send_command(Commands::ROWS_NEXT, ["aa", "101"])
    assert_equal [], unpack_next
    assert_equal [Commands::HASH_NEXT, ["aa", "968116383"], hash_of([["968116383", "aa", "9", "9"]])],
      unpack_next

    assert_equal [Commands::ROWS_CURR, ["aa", "1000000000"]],
     send_command(Commands::ROWS_CURR, ["aa", "1000000000"])
    assert_equal ["968116383", "aa", "9", "9"], unpack_next
    assert_equal [], unpack_next
    assert_equal Commands::HASH_NEXT, unpack_next.first

    assert_equal [Commands::ROWS_CURR, ["ww", "1"]],
     send_command(Commands::ROWS_NEXT, ["ww", "1"])
    assert_equal [], unpack_next
    assert_equal Commands::HASH_NEXT, unpack_next.first

    assert_equal [Commands::ROWS_CURR, ["zz", "1"]],
     send_command(Commands::ROWS_CURR, ["zz", "1"])
    assert_equal ["2349174", "xy", "1", "2"], unpack_next
    assert_equal [], unpack_next
    assert_equal [Commands::ROWS_NEXT, []],
      unpack_next
    assert_equal [], unpack_next
  end

  test_each "supports reserved-word column names" do
    clear_schema
    create_reservedtbl
    send_handshake_commands

    assert_equal [Commands::ROWS_NEXT, []],
     send_command(Commands::OPEN, "reservedtbl")
    assert_equal [], unpack_next

    assert_equal [Commands::ROWS_CURR, []],
     send_command(Commands::ROWS_NEXT, [])
    assert_equal [], unpack_next
  end
end
