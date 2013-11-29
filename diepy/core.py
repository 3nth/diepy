import csv
import logging
from ConfigParser import SafeConfigParser
import time
from datetime import datetime

import sqlalchemy


logger = logging.getLogger(__name__)
    
class dbserver(object):
    
    def __init__(self, server, database = None):
        self.metadata = sqlalchemy.MetaData()
        self.engine = None
        
        logger.info('Parsing config file...')
        parser = SafeConfigParser()
        parser.read('diepy.ini')

        cstring = parser.get('servers', server)
    
        logger.info('Connecting to database...')
        self.engine = sqlalchemy.create_engine(cstring)
        self.metadata.bind = self.engine
    
    def import_file(self, filepath, table_name = None, schema=None, delimiter=','):
        try:
            if not table_name:
                (table_name, ext) = path.splitext(path.basename(filepath))
            
            if not self.table_exists(table_name, schema):
                
                self.create_table(table_name, filepath, delimiter, schema=schema)
            
            table = sqlalchemy.Table(table_name, self.metadata, autoload=True, schema=schema)
            
            rows = self.store_data(filepath, table, delimiter)
            logger.info("Stored %s records from %s in %s" % (rows, filepath, table.name))
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
        cn = self.engine.connect()
        infile = open(filepath, 'rb')
        dr = csv.DictReader(infile, delimiter=delimiter)
        errors = 0
        rows = 0
        batch = []
        for row in dr:
            rows += 1
            record = {k: self.cast_data(k, v, table) for k, v in row.items()}
            batch.append(record)
            if rows % 100 == 0:
                cn.execute(table.insert(), batch)
                batch = []
                print "\r%s records..." % rows,
                sys.stdout.flush()

        print "\r                             \r",
        
        if len(batch) > 0:
            cn.execute(table.insert(), batch)

        if errors > 0:
            raise Exception, "Had trouble storing %s in %s\n%i errors in %i records" % (filepath, table.name, errors, rows )

        return rows
     
    def cast_data(self, k, v, table):
        
        if v is None or v == '':
            return None
        
        logger.info('Attempting to cast %s as %s ...' % (v, table.c[k].type)) 
        if isinstance(table.c[k].type, sqlalchemy.types.DATETIME):
            return self.cast_datetime(v)
        
        return v
    
    def cast_datetime(self, v):
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
            
    def export_table(self, table, filename, schema=None, unix=False, zip=False):
        mytable = sqlalchemy.Table(table, self.metadata, autoload=True, schema=schema)
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
                writer.writerow(cleaned)
                records += 1

        finally:
            f.close()
            
    def _cleanbool(self, value):
        if value is None:
            return value

        if type(value) is bool and value:
            return 1
        if type(value) is bool and not value:
            return 0
        return value
            
    
        
def generate_schema(filepath, delimiter=','):
    '''Generates a table DDL statement based on the file'''
    logger.info("Generating schema for '%s'" % filepath)
    infile = open(filepath, 'rb')
    dr = csv.DictReader(infile, delimiter=delimiter)

    columns = {}
    for row in dr:
        for field in dr.fieldnames:
            if not columns.has_key(field):
                columns[field] = columndef(field)
            columns[field].sample_value(row[field])

    return columns

class columndef(object):
    '''Defines the schema for a table column'''

    def __init__(self, name = ''):
        self.name = name
        self.nullable = False
        self.type = ''
        self.length = 0
        self.max_value = 0

    def sample_value(self, value):
        '''Samples a value to determine the datatype for the column'''

        if value == '' or value is None:
            self.nullable = True
            return

        if len(value) > self.length:
            self.length = len(value)

        if self.is_int(value) and abs(int(value)) > self.max_value:
            self.max_value = abs(int(value))

        self._determine_type(value)

    def _determine_type(self, value):
        if self.type == 'date' and not self.is_date(value):
            self.type = 'text'
        if self.type == 'float' and not self.is_float(value):
            self.type = 'text'
        if self.type == 'int' and not self.is_int(value):
            self.type = 'text'

        if self.type == '':
            if self.is_date(value):
                self.type = 'date'
            elif self.is_int(value):
                self.type = 'int'
            elif self.is_float(value):
                self.type = 'float'
            else:
                self.type = 'text'

    def emit(self):
        '''Prints the DDL for defining the column'''

        if self.type == '':
            self.type = 'text'

        if self.type == 'int' and self.max_value > 32768:
            return sqlalchemy.Column(self.name, sqlalchemy.Integer, nullable=self.nullable)
        elif self.type =='int':
            return sqlalchemy.Column(self.name, sqlalchemy.SmallInteger, nullable=self.nullable)
        elif self.type == 'float':
            return sqlalchemy.Column(self.name, sqlalchemy.Float, nullable=self.nullable)
        elif self.type == 'date':
            return sqlalchemy.Column(self.name, sqlalchemy.DateTime, nullable=self.nullable)
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
                return sqlalchemy.Column(self.name, sqlalchemy.Text, nullable=self.nullable)

    def is_int(self, s):
        try:
            int(s)
            return True
        except ValueError:
            return False

    def is_float(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def is_date(self, s):
        isdate = False
        try:
            time.strptime(s, "%m/%d/%y")
            isdate = True
        except ValueError:
            pass
            
        try:
            time.strptime(s, "%Y-%m-%d")
            isdate = True
        except ValueError:
            pass
        
        return isdate
        