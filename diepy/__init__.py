import logging
from .core import Database
from os import path, listdir
import time

logger = logging.getLogger(__name__)
handler = logging.NullHandler()
logger.addHandler(handler)

def import_files(server, database, schema, table, delimiter, import_paths):

    db = Database(server, database)

    if path.isfile(import_paths):
        db.import_file(import_paths, table, schema, delimiter=delimiter)
        return

    for inpath in import_paths:
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