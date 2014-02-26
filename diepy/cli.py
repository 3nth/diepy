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
from .core import Database, import_files, parse_dbpath

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
        parser.add_argument('--datestamp', action='store_true', help='add a datestamp to the filename (ex: -2014.02.24)')
        parser.add_argument('--timestamp', action='store_true', help='add a datestamp and timestamp to the filename. (ex: -2014.02.24.1345)')
        parser.add_argument('--zip', action='store_true', help='gzip file. You can also add the .gz extension to the dst path to get compression.')
        parser.add_argument('src', action='store', help='Table to export (ie. SERVER.DATABASE.SCHEMA.TABLE)')
        parser.add_argument('dst', action='store', help='Where to export. Defaults to working directory.')
        return parser
        
    def take_action(self, parsed_args):
        if self.app_args.tab:
            delimiter = '\t'
        else:
            delimiter = ','
        
        server, database, schema, table = parse_dbpath(parsed_args.src)
            
        out = parsed_args.dst or os.getcwd()
        
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
        parser.add_argument('src', action='store', nargs='+', help='File(s) to import')
        parser.add_argument('dst', action='store', help='Table name')
        return parser
        
    def take_action(self, parsed_args):
        if self.app_args.tab:
            delimiter = '\t'
        else:
            delimiter = ','

        server, database, schema, table = parse_dbpath(parsed_args.dst)
        
        if not server:
            raise Exception("Need to specify server.")

        self.log.info("Importing...\nFile: {}\nServer: {}\nDatabase: {}\nSchema: {}\nTable: {}".format(parsed_args.src, server, database, schema, table))
        db = Database(server, database, self.app_args.config)

        for src in parsed_args.src:
            if path.isdir(src) and table:
                raise Exception("If importing a directory, don't specify the table name.")

            if path.isfile(src):
                db.import_file(src, table, schema, delimiter=delimiter)
            elif path.isdir(parse_args.src):
                for fpath in [path.join(src, p) for p in os.listdir(src)]:
                    if not fpath.endswith('.csv'):
                        continue
                    db.import_file(fpath, None, schema, delimiter=delimiter)
            else:
                raise Exception('Cannot import %s' % src)
                # for fpath in glob.glob(src):
#                     if not fpath.endswith('.csv'):
#                         continue
#                     db.import_file(fpath, None, schema, delimiter=delimiter)



def main(argv=sys.argv[1:]):
    myapp = DiepyApp()
    return myapp.run(argv)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
