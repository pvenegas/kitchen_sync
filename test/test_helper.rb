require 'rubygems'

require 'test/unit'
require 'fileutils'
require 'mocha/setup'

require 'msgpack'
require 'pg'
require 'mysql2'
require 'openssl'

require 'debugger' if ENV['DEBUG']

require File.expand_path(File.join(File.dirname(__FILE__), 'kitchen_sync_spawner'))
require File.expand_path(File.join(File.dirname(__FILE__), 'test_table_schemas'))

FileUtils.mkdir_p(File.join(File.dirname(__FILE__), 'tmp'))

class PGconn
  def execute(sql)
    async_exec(sql)
  end

  def tables
    query("SELECT tablename FROM pg_tables WHERE schemaname = ANY (current_schemas(false))").collect {|row| row["tablename"]}
  end

  def quote_ident(name)
    self.class.quote_ident(name)
  end
end

class Mysql2::Client
  def execute(sql)
    query(sql)
  end

  def tables
    query("SHOW TABLES").collect {|row| row.values.first}
  end

  def quote_ident(name)
    "`#{name}`"
  end
end

ENDPOINT_DATABASES = {
  "postgresql" => {
    :connect => lambda { |host, port, name, username, password| PGconn.connect(
      host,
      port,
      nil,
      nil,
      name,
      username,
      password)
    }
  },
  "mysql" => {
    :connect => lambda { |host, port, name, username, password| Mysql2::Client.new(
      :host     => host,
      :port     => port.to_i,
      :database => name,
      :username => username,
      :password => password)
    }
  }
}

module Commands
  OPEN = 1
  ROWS_CURR = 2
  ROWS_NEXT = 3
  HASH_CURR = 4
  HASH_NEXT = 5

  PROTOCOL = 32
  EXPORT_SNAPSHOT  = 33
  IMPORT_SNAPSHOT  = 34
  UNHOLD_SNAPSHOT  = 35
  WITHOUT_SNAPSHOT = 36
  SCHEMA = 37
  QUIT = 0
end

Verbs = Commands.constants.each_with_object({}) {|k, results| results[Commands.const_get(k)] = k.to_s.downcase}.freeze

module KitchenSync
  class TestCase < Test::Unit::TestCase
    CURRENT_PROTOCOL_VERSION = 1

    undef_method :default_test if instance_methods.include? 'default_test' or
                                  instance_methods.include? :default_test

    def captured_stderr_filename
      @captured_stderr_filename ||= File.join(File.dirname(__FILE__), 'tmp', 'captured_stderr') unless ENV['NO_CAPTURE_STDERR'].to_i > 0
    end

    def binary_path
      @binary_path ||= File.join(File.dirname(__FILE__), '..', 'build', binary_name)
    end

    def spawner
      @spawner ||= KitchenSyncSpawner.new(binary_path, program_args, :capture_stderr_in => captured_stderr_filename).tap(&:start_binary)
    end

    def unpacker
      spawner.unpacker
    end

    def send_command(*args)
      spawner.send_command(*args)
    end

    def send_handshake_commands
      send_protocol_command
      send_without_snapshot_command
    end

    def send_protocol_command
      assert_equal CURRENT_PROTOCOL_VERSION, send_command(Commands::PROTOCOL, CURRENT_PROTOCOL_VERSION)
    end

    def send_without_snapshot_command
      assert_equal nil, send_command(Commands::WITHOUT_SNAPSHOT)
    end

    def expect_handshake_commands
      # checking how protocol versions are handled is covered in protocol_versions_test; here we just need to get past that to get on to the commands we want to test
      expects(:protocol).with(CURRENT_PROTOCOL_VERSION).returns([CURRENT_PROTOCOL_VERSION])

      # since we haven't asked for multiple workers, we'll always get sent the snapshot-less start command
      expects(:without_snapshot).returns([nil])
    end

    def unpack_next
      spawner.unpack_next
    end

    def receive_commands(*args)
      spawner.receive_commands(*args) do |command|
        verb = Verbs[command[0]] or raise("Unknown command #{command[0]}")
        send(verb, *command[1..-1])
      end
    end

    def expect_stderr(contents)
      spawner.expect_stderr(contents) { yield }
    end
    
    def fixture_file_path(filename)
      File.join(File.dirname(__FILE__), 'fixtures', filename)
    end
  end

  class EndpointTestCase < TestCase
    def binary_name
      "ks_#{@database_server}"
    end

    def database_host
      ENV["#{@database_server.upcase}_DATABASE_HOST"]     || ENV["ENDPOINT_DATABASE_HOST"]     || ""
    end

    def database_port
      ENV["#{@database_server.upcase}_DATABASE_PORT"]     || ENV["ENDPOINT_DATABASE_PORT"]     || ""
    end

    def database_name
      ENV["#{@database_server.upcase}_DATABASE_NAME"]     || ENV["ENDPOINT_DATABASE_NAME"]     || "ks_test"
    end

    def database_username
      ENV["#{@database_server.upcase}_DATABASE_USERNAME"] || ENV["ENDPOINT_DATABASE_USERNAME"] || ""
    end

    def database_password
      ENV["#{@database_server.upcase}_DATABASE_PASSWORD"] || ENV["ENDPOINT_DATABASE_PASSWORD"] || ""
    end

    def program_args
      @program_args ||= [ from_or_to.to_s, database_host, database_port, database_name, database_username, database_password ]
    end

    def connection
      @connection ||= @database_settings[:connect].call(database_host, database_port, database_name, database_username, database_password)
    end

    def execute(sql)
      connection.execute(sql)
    end

    def query(sql)
      connection.query(sql).collect {|row| row.values.collect {|value| value.to_s unless value.nil?}} # one adapter returns strings, the other casts.  for now, we use the lowest common denominator.
    end

    def clear_schema
      connection.tables.each {|table_name| execute "DROP TABLE #{table_name}"}
    end

    def hash_of(rows)
      md5 = OpenSSL::Digest::MD5.new
      md5.digest(rows.collect(&:to_msgpack).join)
    end

    def hash_and_count_of(rows)
      [hash_of(rows), rows.size]
    end

    def self.test_each(description, &block)
      ENDPOINT_DATABASES.each do |database_server, settings|
        define_method("test #{description} for #{database_server}".gsub(/\W+/,'_').to_sym) do
          @database_server = database_server
          @database_settings = settings
          begin
            skip "pending" unless block
            instance_eval(&block)
          ensure
            @spawner.stop_binary if @spawner
            @spawner = nil
            @connection.close rescue nil if @connection
          end
        end if ENV['ENDPOINT_DATABASES'].nil? || ENV['ENDPOINT_DATABASES'].include?(database_server)
      end
    end
  end
end
