
import logging
import os
from os import path
from optparse import OptionParser

import diepy

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

parser = OptionParser()
parser.add_option("-s", "--server", dest="server", default="MNEMOSYNE", help="Database to connect to")
parser.add_option("-d", "--database", dest="database", help="Database to connect to.")
parser.add_option("-c", "--schema", dest="schema", default="dbo", help="Schema name")
parser.add_option("-t", "--table", dest="table", help="Table name")
parser.add_option("--tab", dest="tab", action="store_true", default=False, help="Delimiter")

(options, args) = parser.parse_args()


if options.tab:
    delimiter = '\t'
else:
    delimiter = ','

db = diepy.dbserver()
db.connect(options.server, options.database)

for inpath in args:
    if path.isdir(inpath):
        for name in os.listdir(inpath):
            if not name.endswith('.csv'):
                continue
            print 'Importing: ' + name
            db.store_file(path.join(inpath, name), options.schema, delimiter=delimiter)
    elif path.isfile(inpath):
        db.store_file(inpath, options.schema, options.table, delimiter=delimiter)
    






