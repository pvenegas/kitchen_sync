Installing Kitchen Sync
=======================

To compile Kitchen Sync, you will need:
* a C++11 compiler
* cmake
* boost headers
* openssl library headers
* postgresql client library headers; and/or
* mysql client library headers

Kitchen Sync needs at least one database client library to do something useful, but it will produce a separate binary for each different database, so you don't need to compile or deploy all the binaries on systems where you won't use them.

Ubuntu
------

You can install these on Ubuntu using:
```
apt-get install build-essential cmake libboost-program-options-dev libssl-dev
```

And one or both of:
```
apt-get install libpq-dev
apt-get install libmysqlclient-dev
```

OS X
----

Homebrew users can install these on OS X using:
```
brew install cmake boost
```

And one or both of:
```
brew install postgresql
brew install mysql
```

To build, change to the kitchen_sync directory where you checked out the files, then:
```
  cd build
  cmake .. && make && make install
```

Next steps
----------

Please see [Using Kitchen Sync](USAGE.md) to get started.

If you'd like to check everything is working first, or submit patches to Kitchen Sync, please see [Running the test suite](TESTS.md).
