import logging
from argparse import ArgumentParser
from diepy import import_files, export_table

def run():
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    parser = ArgumentParser()
    # group = parser.add_mutually_exclusive_group()
    # group.add_argument('-x', dest="command", action='store_const', const='export')
    # group.add_argument('-i', dest="command", action='store_const', const='import')
    parser.add_argument("command", action="store")
    parser.add_argument("-s", "--server", dest="server", help="Database Server to connect to")
    parser.add_argument("-d", "--database", dest="database", help="Database to connect to.")
    parser.add_argument("-c", "--schema", dest="schema", help="Schema name")
    parser.add_argument("-t", "--table", dest="table", help="Table name")
    parser.add_argument("--tab", dest="tab", action="store_true", default=False, help="Delimiter")
    parser.add_argument("--config", dest="config", help="A path to a specific config file to use")
    parser.add_argument("files", action="store")

    options = parser.parse_args()

    if options.tab:
        delimiter = '\t'
    else:
        delimiter = ','

    if options.command == 'import':
        import_files(options.server,
                          options.database,
                          options.schema,
                          options.table,
                          delimiter,
                          options.files,
                          options.config)

    elif options.command == 'export':
        export_table(options.server,
                          options.database,
                          options.schema,
                          options.table,
                          options.files[0])



