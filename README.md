## diepy is

a stinky wrapper for you database needs

## diepy needs

#### connection strings

these are specified in a configuration file. Configuration file found/used in this order:
1. --config argument. If passed, this is the only file attempted.
2. A "diepy.ini" file in the cwd
3. A "diepy.ini" file in the users home folder.

The configuration file has a single section, servers, which has a key = connection string listing of servers. The -s argument corresponds to a key here. The connection strings are used by SqlAlchemy so refer to their doc on how to write one.

If database (-d) is passed then it's appended to the connection string "cstring/database"

## diepy does

#### import

import a csv file

	diepy import -s SERVER -d DATABASE -c SCHEMA -t TABLE path/to/some/file.csv

or a tab delimited file

	diepy import -s SERVER -d DATABASE -c SCHEMA -t TABLE --tab path/to/some/file.csv

or a whole directory of files

	diepy import -s SERVER -d DATABASE -c SCHEMA -t TABLE path/to/some/files/

let diepy create a table named the same as the file

	diepy import -s SERVER -d DATABASE -c SCHEMA path/to/some/file.csv

#### export

export a table to a csv file

	diepy export -s SERVER -d DATABASE -c SCHEMA -t TABLE path/to/some/file.csv



