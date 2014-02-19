import argparse
import datetime
import logging
import os
from os import path
import sys

from cliff.app import App
from cliff.command import Command
from cliff.commandmanager import CommandManager

import diepy
from .core import Database, import_files

class DiepyApp(App):
    
    log = logging.getLogger(__name__)
    
    def __init__(self):
        super(DiepyApp, self).__init__(
            description='diepy',
            version=diepy.__version__,
            command_manager=CommandManager('diepy'),
        )
    
    def build_option_parser(self, description, version, argparse_kwargs=None):
        parser = super(DiepyApp, self).build_option_parser(description, version, argparse_kwargs)
        parser.add_argument('--config', action='store', default=None, help='Path to config file.')
        parser.add_argument('-s', '--server', dest='server', help='Database Server to connect to')
        parser.add_argument('-d', '--database', dest='database', help='Database to connect to.')
        parser.add_argument('--tab', dest='tab', action='store_true', default=False, help='Delimiter')
        
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
    
    def get_parser(self, prog_name):
        parser = argparse.ArgumentParser()
        parser.add_argument('--unix', action='store_true', help='Use unix line endings')
        parser.add_argument('--datestamp', action='store_true', help='add a datestamp to the filename')
        parser.add_argument('--timestamp', action='store_true', help='add a datestamp and timestamp to the filename.')
        parser.add_argument('--zip', action='store_true', help='zip/gzip file')
        parser.add_argument('table', action='store', help='Table name')
        parser.add_argument('out', action='store', help='Export file')
        return parser
        
    def take_action(self, parsed_args):
        if self.app_args.tab:
            delimiter = '\t'
        else:
            delimiter = ','
        
        server = self.app_args.server
        database = self.app_args.database
        schema = self.app_args.schema
        
        parts = parsed_args.table.split('.')

        if len(parts) == 2:
            schema = parts[0]
            table = parts[1]
        elif len(parts) == 3:
            database = parts[0]
            schema = parts[1]
            table = parts[2]
        elif len(parts) == 4:
            server = parts[0]
            database = parts[1]
            schema = parts[2]
            table = parts[3]
            
        out = parsed_args.out or os.getcwd()
        
        zip = parsed_args.zip or out.endswith('.gz')
        
        if out.endswith('.csv') or out.endswith('.gz'):
            fname = path.basename(out)
            fname = fname.replace('.csv', '').replace('.gz', '')
            out = path.dirname(out)
        else:
            fname = table
        
        if parsed_args.datestamp or parsed_args.timestamp:
            fname = '{}-{:%Y.%m.%d}'.format(fname, datetime.datetime.now())
        
        if parsed_args.timestamp:
            fname = '{}.{:%H%M}'.format(fname, datetime.datetime.now())
        
        out = path.join(out, fname + '.csv')
        
        db = Database(
                server,
                database,
                self.app_args.config
        )
        db.export_table(
            table,
            out,
            schema,
            parsed_args.unix,
            zip
        )


class Import(Command):
    "Import data."
    
    log = logging.getLogger(__name__)
    
    def get_parser(self, prog_name):
        parser = argparse.ArgumentParser()
        parser.add_argument('files', action='store', help='File(s) to import')
        parser.add_argument('dst', action='store', help='Table name')
        return parser
        
    def take_action(self, parsed_args):
        if self.app_args.tab:
            delimiter = '\t'
        else:
            delimiter = ','

        server = self.app_args.server
        database = self.app_args.database
        schema = None
        table = None
        
        parts = parsed_args.dst.split('.')


        if len(parts) == 1:
            server = parts[0]
        elif len(parts) == 2:
            server = parts[0]
            database = parts[1]
        elif len(parts) == 3:
            server = parts[0]
            database = parts[1]
            schema = parts[2]
        elif len(parts) == 3:
            server = parts[0]
            database = parts[1]
            schema = parts[2]
        elif len(parts) == 4:
            server = parts[0]
            database = parts[1]
            schema = parts[2]
            table = parts[3]

        if path.isdir(parsed_args.files) and table:
            raise Exception("If importing a directory, don't specify the table name.")
        
        if not server:
            raise Exception("Need to specify server.")
            
        import_files(
            parsed_args.files,
            server,
            database,
            schema,
            table,
            delimiter,
            self.app_args.config
        )


def main(argv=sys.argv[1:]):
    myapp = DiepyApp()
    return myapp.run(argv)
    
if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
