require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SchemaToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  test_each "accepts an empty list of tables on an empty database" do
    clear_schema

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => []
    expect_command Commands::QUIT
  end

  test_each "accepts a matching list of tables with matching schema" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::QUIT
  end


  test_each "complains about a non-empty list of tables on an empty database" do
    clear_schema

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing table footbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def]
      read_command rescue nil      
    end
  end

  test_each "drops tables to match an empty list of tables on a non-empty database" do
    clear_schema
    create_footbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => []
    read_command
    assert_equal %w(), connection.tables
  end

  test_each "complains about a missing table before other tables" do
    clear_schema
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing table footbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
      read_command rescue nil      
    end
  end

  test_each "complains about a missing table between other tables" do
    clear_schema
    create_footbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing table middletbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
      read_command rescue nil      
    end
  end

  test_each "complains about a missing table after other tables" do
    clear_schema
    create_footbl
    create_middletbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing table secondtbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
      read_command rescue nil      
    end
  end

  test_each "drops extra tables before other tables" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [middletbl_def, secondtbl_def]
    read_command
    assert_equal %w(middletbl secondtbl), connection.tables
  end

  test_each "drops extra tables between other tables" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def, secondtbl_def]
    read_command
    assert_equal %w(footbl secondtbl), connection.tables
  end

  test_each "drops extra tables after other tables" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def]
    read_command
    assert_equal %w(footbl middletbl), connection.tables
  end


  test_each "doesn't complain about a missing table before other tables if told to ignore the table, and doesn't ask for its data" do
    program_args << 'footbl'
    clear_schema
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    read_command
  end

  test_each "doesn't complain about a missing table between other tables if told to ignore the table" do
    program_args << 'middletbl'
    clear_schema
    create_footbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    read_command
  end

  test_each "doesn't complain about a missing table after other tables if told to ignore the table" do
    program_args << 'secondtbl'
    clear_schema
    create_footbl
    create_middletbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    read_command
  end

  test_each "doesn't complain about extra tables before other tables if told to ignore the table" do
    program_args << 'footbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    read_command
  end

  test_each "doesn't complain about extra tables between other tables if told to ignore the table" do
    program_args << 'middletbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    read_command
  end

  test_each "doesn't complain about extra tables after other tables if told to ignore the table" do
    program_args << 'secondtbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    read_command
  end


  test_each "complains about missing columns before other columns" do
    clear_schema
    create_secondtbl
    execute("ALTER TABLE secondtbl DROP COLUMN tri")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing column tri on table secondtbl") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def]
      read_command rescue nil      
    end
  end

  test_each "complains about missing columns between other columns" do
    clear_schema
    create_footbl
    execute("ALTER TABLE footbl DROP COLUMN another_col")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing column another_col on table footbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def]
      read_command rescue nil      
    end
  end

  test_each "complains about missing columns after other columns" do
    clear_schema
    create_footbl
    execute("ALTER TABLE footbl DROP COLUMN col3")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing column col3 on table footbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def]
      read_command rescue nil      
    end
  end

  test_each "drops extra columns before other columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    columns = footbl_def["columns"][1..-1]
    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def.merge("columns" => columns)]
    read_command
    assert_equal columns.collect {|column| column["name"]}, connection.table_column_names("footbl")
  end

  test_each "drops extra columns between other columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    columns = footbl_def["columns"][0..0] + footbl_def["columns"][2..-1]
    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def.merge("columns" => columns)]
    read_command
    assert_equal columns.collect {|column| column["name"]}, connection.table_column_names("footbl")
  end

  test_each "drops extra columns after other columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    columns = footbl_def["columns"][0..-2]
    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def.merge("columns" => columns)]
    read_command
    assert_equal columns.collect {|column| column["name"]}, connection.table_column_names("footbl")
  end

  test_each "complains about misordered columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Misordered column col3 on table footbl, should have another_col first") do
      send_command Commands::SCHEMA, "tables" => [footbl_def.merge("columns" => footbl_def["columns"][0..0] + footbl_def["columns"][2..-1] + footbl_def["columns"][1..1])]
      read_command rescue nil
    end
  end


  test_each "complains about column types not matching" do
    clear_schema
    create_footbl
    execute({"mysql" => "ALTER TABLE footbl MODIFY another_col VARCHAR(11)", "postgresql" => "ALTER TABLE footbl ALTER COLUMN another_col TYPE VARCHAR(11)"}[@database_server])

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Column another_col on table footbl should have type INT but has type VARCHAR") do
      send_command Commands::SCHEMA, "tables" => [footbl_def]
      read_command rescue nil      
    end
  end

  test_each "complains about column nullability not matching" do
    clear_schema
    create_footbl
    execute({"mysql" => "ALTER TABLE footbl MODIFY another_col SMALLINT NOT NULL", "postgresql" => "ALTER TABLE footbl ALTER COLUMN another_col SET NOT NULL"}[@database_server])

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Column another_col on table footbl should be nullable but is not nullable") do
      send_command Commands::SCHEMA, "tables" => [footbl_def]
      read_command rescue nil      
    end
  end


  test_each "complains if the primary key column order doesn't match" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Mismatching primary key (pri2, pri1) on table secondtbl, should have (pri1, pri2)") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("primary_key_columns" => [1, 2])]
      read_command rescue nil      
    end
  end

  test_each "complains if there are extra primary key columns after the matching part" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Mismatching primary key (pri2, pri1) on table secondtbl, should have (pri2, pri1, sec)") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("primary_key_columns" => [2, 1, 3])]
      read_command rescue nil      
    end
  end

  test_each "complains if there are extra primary key columns before the matching part" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Mismatching primary key (pri2, pri1) on table secondtbl, should have (sec, pri2, pri1)") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("primary_key_columns" => [3, 2, 1])]
      read_command rescue nil      
    end
  end


  test_each "drops extra keys" do
    clear_schema
    create_secondtbl
    execute "CREATE INDEX extrakey ON secondtbl (sec, tri)"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [secondtbl_def]
    read_command
    assert_equal secondtbl_def["keys"].collect {|key| key["name"]}, connection.table_keys("secondtbl")
  end

  test_each "complains about missing keys" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing key missingkey on table secondtbl") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("keys" => secondtbl_def["keys"] + [secondtbl_def["keys"][0].merge("name" => "missingkey")])]
      read_command rescue nil      
    end
  end

  test_each "complains about keys whose unique flag doesn't match" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Mismatching unique flag on table secondtbl key secidx") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("keys" => [secondtbl_def["keys"][0].merge("unique" => true)])]
      read_command rescue nil      
    end
  end

  test_each "complains about about column list differences on keys" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Mismatching columns (sec) on table secondtbl key secidx, should have (sec, pri1)") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("keys" => [secondtbl_def["keys"][0].merge("columns" => [3, 1])])]
      read_command rescue nil      
    end
  end
end
