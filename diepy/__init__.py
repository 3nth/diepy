import logging
from .core import Database
from os import path, listdir
import time

__version__ = '0.2'

logger = logging.getLogger(__name__)
handler = logging.NullHandler()
logger.addHandler(handler)

def import_files(server, database, schema, table, delimiter, import_paths, config):

    db = Database(server, database, config)

    if path.isfile(import_paths):
        db.import_file(import_paths, table, schema, delimiter=delimiter)
        return

    for inpath in listdir(import_paths):
        logger.info("Importing: %s" % inpath)
        time.sleep(5)
        if path.isdir(inpath):
            for name in listdir(inpath):
                if not name.endswith('.csv'):
                    continue
                print 'Importing: ' + name
                db.import_file(path.join(inpath, name), name, schema, delimiter=delimiter)



def export_table(server, database, schema, table, export_path):
    db = Database(server, database)
    db.export_table(table, export_path)