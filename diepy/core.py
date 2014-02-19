import csv
from ConfigParser import SafeConfigParser
from datetime import datetime
import gzip
import logging
import os
from os import path
import sys
import time

from dateutil.parser import parse
import sqlalchemy

logger = logging.getLogger(__name__)


def import_files(import_path, server, database=None, schema=None, table=None, delimiter=None, config=None):
    """Import file(s) into database.
    
    Args:
        server (string): the server to connect to. Should be in config file.
        database: the database on the server to connect to.
        schema: the schema of the table.
        table: the name of the table
        delimiter: the delimiter to use when parsing the text file. default is comma
        import_paths: either a single file or a directory of files.
        config: path to a specific configuration file to use.
    
    Returns:
        nothing
    
    """
    db = Database(server, database, config)

    if path.isfile(import_path):
        db.import_file(import_path, table, schema, delimiter=delimiter)
        return

    for fpath in [path.join(import_path, p) for p in os.listdir(import_path)]:
        if not fpath.endswith('.csv'):
            continue
        logger.info("Importing: %s" % fpath)
        db.import_file(fpath, None, schema, delimiter=delimiter)


def export_table(server, database, schema, table, export_path):
    db = Database(server, database)
    db.export_table(table, export_path)


class Database(object):
    def __init__(self, server, database=None, config=None):
        self.metadata = sqlalchemy.MetaData()
        self.engine = None

        parser = SafeConfigParser()

        if config:
            logger.info('Parsing config file %s' % config)
            parser.read(config)
        elif path.exists('diepy.ini'):
            logger.info('Parsing config file %s' % path.abspath('diepy.ini'))
            parser.read('diepy.ini')
        elif path.exists(path.expanduser("~/diepy.ini")):
            logger.info('Parsing config file %s' % path.abspath(path.expanduser('~/diepy.ini')))
            parser.read(path.abspath(path.expanduser('~/diepy.ini')))
        else:
            raise Exception("No configuration file found!")

        cstring = parser.get('servers', server)

        if database:
            cstring = cstring.rstrip('/') + '/' + database

        logger.info('Connecting to database: %s' % cstring)
        self.engine = sqlalchemy.create_engine(cstring)
        self.metadata.bind = self.engine

    def import_file(self, filepath, table_name=None, schema=None, delimiter=','):
        try:
            if not table_name:
                (table_name, ext) = path.splitext(path.basename(filepath))

            if not self.table_exists(table_name, schema):
                self.create_table(table_name, filepath, delimiter, schema=schema)

            table = sqlalchemy.Table(table_name, self.metadata, autoload=True, schema=schema)

            rows = self.store_data(filepath, table, delimiter)

        except:
            logger.exception("Had some trouble storing %s" % filepath)

    def table_exists(self, table_name, schema=None):
        exists = self.engine.dialect.has_table(self.engine.connect(), table_name, schema=schema)
        if not exists:
            logger.warn("Table '%s' does not exist." % table_name)
        return exists

    def create_table(self, table_name, filepath, delimiter, schema=None):
        logger.info("Creating table for '%s' ..." % filepath)
        table = sqlalchemy.Table(table_name, self.metadata, schema=schema)
        for k, v in generate_schema(filepath, delimiter).items():
            table.append_column(v.emit())

        table.create(self.engine)

    def store_data(self, filepath, table, delimiter=','):
        logger.info("Storing records from %s in %s" % (filepath, table.name))
        cn = self.engine.connect()
        infile = open(filepath, 'rb')
        dr = csv.DictReader(infile, delimiter=delimiter)
        rows = 0
        batch = []
        for row in dr:
            rows += 1
            record = {k: cast_data(k, v, table) for k, v in row.items()}
            batch.append(record)
            if rows % 100 == 0:
                cn.execute(table.insert(), batch)
                logger.info("Imported %s records..." % rows)
                batch = []

        if batch:
            cn.execute(table.insert(), batch)

        logger.info("Stored %s records from %s in %s" % (rows, filepath, table.name))
        return rows

    def export_table(self, table, filename, schema=None, unix=False, zip=False):
        mytable = sqlalchemy.Table(table, self.metadata, autoload=True, schema=schema or None)
        db_connection = self.engine.connect()

        select = sqlalchemy.sql.select([mytable])
        result = db_connection.execute(select)

        if zip:
            if not filename.endswith('.gz'):
                filename += '.gz'
            f = gzip.open(filename, 'wb')
        else:
            f = open(filename, 'wb')

        try:
            if unix:
                lineterminator = '\n'
            else:
                lineterminator = '\r\n'

            writer = csv.writer(f, lineterminator=lineterminator)
            writer.writerow(result.keys())
            records = 0
            for row in result:
                cleaned = [self._cleanbool(x) for x in row]
                writer.writerow(row)
                records += 1

        finally:
            f.close()

    @staticmethod
    def _cleanbool(value):
        if value is None:
            return value

        if type(value) is bool and value:
            return 1
        if type(value) is bool and not value:
            return 0
        if type(value) is datetime:
            return value.isoformat()
        return value


def cast_data(k, v, table):

    if v is None or v == '':
        return None

    logger.debug('Attempting to cast %s as %s ...' % (v, table.c[k].type))
    if isinstance(table.c[k].type, sqlalchemy.types.DATETIME) or isinstance(table.c[k].type, sqlalchemy.types.DATE):
        v = parse(v)

    if isinstance(table.c[k].type, sqlalchemy.types.TIME):
        dt = parse(v)
        v = dt.time()

    logger.debug(v)
    return v


def cast_datetime(v):
    logger.info('Attempting to cast %s as datetime...' % v)
    try:
        return datetime.strptime(v, "%m/%d/%y")
    except ValueError:
        pass

    try:
        return datetime.strptime(v, "%Y-%m-%d")
    except ValueError:
        pass

    return v


def generate_schema(filepath, delimiter=','):
    """Generates a table DDL statement based on the file"""
    logger.info("Generating schema for '%s'" % filepath)
    infile = open(filepath, 'rb')
    dr = csv.DictReader(infile, delimiter=delimiter)

    columns = {}
    for row in dr:
        for field in dr.fieldnames:
            if not field in columns:
                columns[field] = ColumnDef(field)
            columns[field].sample_value(row[field])

    return columns


class ColumnDef(object):
    """Defines the schema for a table column"""

    def __init__(self, name=''):
        self.name = name or 'unnamed'
        self.nullable = False
        self.type = ''
        self.length = 0
        self.max_value = 0
        self.min_value = 0

    def sample_value(self, value):
        """Samples a value to determine the datatype for the column"""

        if value == '' or value is None:
            self.nullable = True
            return

        if len(value) > self.length:
            self.length = len(value)

        if is_int(value) and int(value) < self.min_value:
            self.min_value = int(value)
            
        if is_int(value) and int(value) > self.max_value:
            self.max_value = int(value)

        self._determine_type(value)

    def _determine_type(self, value):
        if self.type == 'date' and not is_date(value):
            self.type = 'text'
        if self.type == 'float' and not is_float(value):
            self.type = 'text'
        if self.type == 'int' and not is_int(value):
            self.type = 'text'

        if self.type == '':
            if is_int(value):
                self.type = 'int'
            elif is_float(value):
                self.type = 'float'
            elif is_date(value):
                self.type = 'date'
            elif is_time(value):
                self.type = 'time'
            elif is_datetime(value):
                self.type = 'datetime'
            else:
                self.type = 'text'

    def emit(self):
        """Prints the DDL for defining the column"""
        logger.debug("emitting '%s' as '%s'. Nullable: %s" % (self.name, self.type, self.nullable))
        if self.type == '':
            self.type = 'text'

        if self.type == 'int' and self.max_value > 32768:
            return sqlalchemy.Column(self.name, sqlalchemy.types.INT, nullable=self.nullable)
        elif self.type == 'int':
            return sqlalchemy.Column(self.name, sqlalchemy.types.SMALLINT, nullable=self.nullable)
        elif self.type == 'float':
            return sqlalchemy.Column(self.name, sqlalchemy.types.FLOAT, nullable=self.nullable)
        elif self.type == 'datetime':
            return sqlalchemy.Column(self.name, sqlalchemy.types.DATETIME, nullable=self.nullable)
        elif self.type == 'date':
            return sqlalchemy.Column(self.name, sqlalchemy.types.DATE, nullable=self.nullable)
        elif self.type == 'time':
            return sqlalchemy.Column(self.name, sqlalchemy.types.TIME, nullable=self.nullable)
        elif self.type == 'text':
            if self.length < 50:
                return sqlalchemy.Column(self.name, sqlalchemy.String(50), nullable=self.nullable)
            elif self.length < 100:
                return sqlalchemy.Column(self.name, sqlalchemy.String(100), nullable=self.nullable)
            elif self.length < 200:
                return sqlalchemy.Column(self.name, sqlalchemy.String(200), nullable=self.nullable)
            elif self.length < 500:
                return sqlalchemy.Column(self.name, sqlalchemy.String(500), nullable=self.nullable)
            elif self.length < 1000:
                return sqlalchemy.Column(self.name, sqlalchemy.String(1000), nullable=self.nullable)
            elif self.length < 4000:
                return sqlalchemy.Column(self.name, sqlalchemy.String(4000), nullable=self.nullable)
            else:
                return sqlalchemy.Column(self.name, sqlalchemy.types.TEXT, nullable=self.nullable)


def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        logger.debug("Value is not an INT: %s" % s)
        return False


def is_float(s):
    try:
        float(s)
        logger.debug("IS A FLOAT: %s" % s)
        return True
    except ValueError:
        logger.debug("Value is not a FLOAT: %s" % s)
        return False


def is_time(s):
    try:
        # parse with two different default dates
        d1 = datetime(2000, 01, 01, 12, 34, 56, 123456)
        d2 = datetime(2007, 10, 20, 14, 32, 12, 654321)
        v1 = parse(s, default=d1)
        v2 = parse(s, default=d2)

        # if the year/month/day end up matching both default dates, then we got time only
        if d1.timetuple()[:3] == v1.timetuple()[:3] and d2.timetuple()[:3] == v2.timetuple()[:3]:
            return True
    except:
        pass

    logger.debug("Value is not a TIME: %s" % s)
    return False


def is_date(s):
    try:
        # parse with two different default dates
        d1 = datetime(2000, 01, 01, 12, 34, 56)
        d2 = datetime(2007, 10, 20, 14, 32, 12)
        v1 = parse(s, default=d1)
        v2 = parse(s, default=d2)

        # if the hour, minute, second, microseconds end up matching both default dates, then we got date only
        if d1.timetuple()[3:6] == v1.timetuple()[3:6] and d2.timetuple()[3:6] == v2.timetuple()[3:6]:
            return True
        if parse(s).timetuple()[3:6] == (0, 0, 0):
            return True
    except:
        pass

    logger.debug("Value is not a DATE: %s" % s)
    return False


def is_datetime(s):
    try:
        # parse with two different default dates
        d1 = datetime(2000, 01, 01, 12, 34, 56, 123456)
        d2 = datetime(2007, 10, 20, 14, 32, 12, 654321)
        v1 = parse(s, default=d1)
        v2 = parse(s, default=d2)

        # if if doesn't match either, then we've got a date time
        if d1 != v1 and d2 != v2:
            logger.debug("IS A DATETIME: %s" % s)
            return True
    except:
        pass

    logger.debug("Value is not a DATETIME: %s" % s)
    return False
