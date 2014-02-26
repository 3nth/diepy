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

#### db table references
SERVER.DATABASE.SCHEMA.TABLE

SERVER is always required. it's the used to look up the connection string in the config file.
DATABASE is optional. will use default database (which can be configured via the connection string).
SCHEMA is optional. Will use default schema.
TABLE is required for exports. If not specified for import, the filename is used. TABLE cannot be used when importing a directory of files. In that case, the filename is used.

If you want to leave out an adjoining element, you can do this:
SERVER...TABLE

#### import

import a csv file

	diepy import SERVER.DATABASE.SCHEMA.TABLE path/to/some/file.csv

or a tab delimited file

	diepy import SERVER.DATABASE.SCHEMA.TABLE --tab path/to/some/file.csv

or a whole directory of files

	diepy import path/to/some/files/ SERVER.DATABASE.SCHEMA 

let diepy create a table named the same as the file

	diepy import path/to/some/file.csv SERVER.DATABASE.SCHEMA 

#### export

export a table to a csv file

	diepy export SERVER.DATABASE.SCHEMA.TABLE path/to/some/file.csv

export a table to a tab delimited file

	diepy export SERVER.DATABASE.SCHEMA.TABLE path/to/some/file.tsv
    diepy export SERVER.DATABASE.SCHEMA.TABLE path/to/some/file.tab

export a table to a gzip'd csv file

	diepy export SERVER.DATABASE.SCHEMA.TABLE path/to/some/file.csv.gz

export to an xlsx file

    diepy export SERVER.DATABASE.SCHEMA.TABLE path/to/some/file.xlsx

xlsx exports will name the sheets using the table name.

