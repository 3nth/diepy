diepy
=====

## diepy is

a stinky wrapper for you database needs

## diepy needs

#### login information

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



