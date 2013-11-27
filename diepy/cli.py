import logging
from optparse import OptionParser

from . import core

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

parser = OptionParser()
parser.add_option("command", dest="command")
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

if options.command == 'import':
    core.import_files(options.server,
                      options.database,
                      options.schema,
                      options.table,
                      delimiter,
                      args)

elif options.command == 'export':
    core.export_table(options.server,
                      options.database,
                      options.schema,
                      options.table,
                      args[0])



