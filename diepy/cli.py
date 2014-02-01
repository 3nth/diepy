import logging
import sys

from cliff.app import App
from cliff.command import Command
from cliff.commandmanager import CommandManager

from .core import diepy

class DiepyApp(App):
    
    log = logging.getLogger(__name__)
    
    def __init__(self):
        super(DiepyApp, self).__init__(
            description='diepy',
            version='0.3',
            command_manager=CommandManager('diepy'),
        )
    
    def build_option_parser(self, description, version, argparse_kwargs=None):
        parser = super(QualpyApp, self).build_option_parser(description, version, argparse_kwargs)
        parser.add_argument('--config', action='store', default=None, help='Path to config file.')
        parser.add_argument("-s", "--server", dest="server", help="Database Server to connect to")
        parser.add_argument("-d", "--database", dest="database", help="Database to connect to.")
        parser.add_argument("-c", "--schema", dest="schema", help="Schema name")
        parser.add_argument("-t", "--table", dest="table", help="Table name")
        parser.add_argument("--tab", dest="tab", action="store_true", default=False, help="Delimiter")
        parser.add_argument("files", action="store")
        return parser
        
    def initialize_app(self, argv):
        self.log.debug('initialize_app')

    def prepare_to_run_command(self, cmd):
        self.log.debug('prepare_to_run_command %s', cmd.__class__.__name__)

    def clean_up(self, cmd, result, err):
        self.log.debug('clean_up %s', cmd.__class__.__name__)
        if err:
            self.log.debug('got an error: %s', err)


class Export(Command):
    """Export data."""
    
    log = logging.getLogger(__name__)
    
    def take_action(self, parsed_args):
        if parsed_args.tab:
            delimiter = '\t'
        else:
            delimiter = ','
        
        export_table(options.server,
                          options.database,
                          options.schema,
                          options.table,
                          options.files[0])


class Import(Command):
    "Import data."
    
    log = logging.getLogger(__name__)
    
    def take_action(self, parsed_args):
        if parsed_args.tab:
            delimiter = '\t'
        else:
            delimiter = ','

        import_files(parsed_args.server,
                          parsed_args.database,
                          parsed_args.schema,
                          parsed_args.table,
                          delimiter,
                          parsed_args.files,
                          parsed_args.config)


def main(argv=sys.argv[1:]):
    myapp = DiepyApp()
    return myapp.run(argv)
    
if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
