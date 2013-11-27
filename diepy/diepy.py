__author__ = 'VHASFCFLENND'
import csv
import pyodbc
import logging
import subprocess
from os import path
import time
import gzip
import shutil
import sys

logger = logging.getLogger('diepy')

class dbserver(object):

    connected = False

    def __init__(self):
        self._cn = None

    def connect(self, server, database=None,username=None,password=None,trusted=False):
        # connectionString = ( r'DRIVER={SQL Server};Server=%s;Database=%s;UID=%s;PWD=%s' % (server, database, user, password))
        connectionString = ( r'DRIVER={SQL Server};Server=%s;Database=%s;Trusted_Connection=yes' % (server, database))

        logger.info("Connecting to %s on %s" % (database, server))
        try:
            self._cn = pyodbc.connect(connectionString)
            self.connected = True
        except:
            logger.exception("Unable to connect to %s on %s" % (database, server))

    def store_file(self, filepath, schema, table = None, delimiter=','):
        strip_nul_bytes(filepath)
        try:
            if not table:
                (table, ext) = path.splitext(path.basename(filepath))

            cursor = self._cn.cursor()

            if not self.table_exists(schema, table):
                sql = generate_schema(schema, table, filepath, delimiter)
                cursor.execute(sql)
                self._cn.commit()
            rows = self.store_data(filepath, schema, table,delimiter)
            logger.info("Stored %s records from %s in %s.%s" % (rows, filepath, schema, table))
        except:
            logger.exception("Had some trouble storing %s" % filepath)

    def table_exists(self, schema, table):
        cursor = self._cn.cursor()
        sql = "SELECT OBJECT_ID(N'%s.%s', N'U')" % (schema, table)
        logger.debug(sql)
        results = cursor.execute(sql).fetchone()
        if results[0]:
            return True
        return False

    def dump_table_to_csv(self, database, schema, table, filename, unix=False, zip=False):

        if zip:
            if not filename.endswith('.gz'):
                filename += '.gz'
            f = gzip.open(filename, 'wb')
        else:
            f = open(filename, 'wb')

        records = 0

        try:
            if unix:
                lineterminator = '\n'
            else:
                lineterminator = '\r\n'

            writer = csv.writer(f, lineterminator=lineterminator)

            sql = "SELECT * FROM %s.%s.%s" % (database, schema, table)
            cursor = self._cn.cursor()
            cursor.execute(sql)
            rowcount = cursor.rowcount
            writer.writerow([x[0] for x in cursor.description])

            for row in cursor:
                cleaned = [self._cleanbool(x) for x in row]
                writer.writerow(cleaned)
                records += 1

        finally:
            f.close()

        return records

    def _cleanbool(self, value):
        if value is None:
            return value

        if type(value) is bool and value:
            return 1
        if type(value) is bool and not value:
            return 0
        return value

    def dump_table_to_excel(self, tablename, filename):
        cmd = ['\\\\sfc-9lrba_vs1\\MRSU_Central\\Database\\Transfers\\Tools\\ExportToExcel2\\ExportToExcel.exe', '--table', tablename, '--filename', filename]
        logger.debug(subprocess.list2cmdline(cmd))
        proc = subprocess.Popen(cmd, shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE
                                )
        (output, error) = proc.communicate()
        if error:
            logger.error(error)
        if output:
            logger.info(output)

    def store_data(self, filepath, schema, table, delimiter=','):

        cursor = self._cn.cursor()
        cursor.execute("TRUNCATE TABLE [%s].[%s]" % (schema, table))
        infile = open(filepath, 'rb')
        dr = csv.DictReader(infile, delimiter=delimiter)
        errors = 0
        rows = 0
        sql = None
        for row in dr:
            rows += 1
            if not sql:
                sql = "INSERT INTO [%s].[%s] (%s) VALUES (%s)" % (schema, table, "[" + "], [".join(dr.fieldnames) + "]", ", ".join(["?" for field in dr.fieldnames]))
                logger.debug(sql)
            values = [row[field] if row[field] != '' else None for field in dr.fieldnames]
            try:
                cursor.execute(sql, values)
            except:
                errors += 1
                logger.exception(row)

            if rows % 100 == 0:
                self._cn.commit()
                print "\r%s records..." % rows,
                sys.stdout.flush()

        print "\r                             \r",
        self._cn.commit()

        if errors > 0:
            raise Exception, "Had trouble storing %s in %s.%s\n%i errors in %i records" % (filepath, schema, table, errors, rows )

        return rows

def strip_nul_bytes(file):
    orig = file + '.orig'
    shutil.move(file, orig)
    fi = open(orig, 'rb')
    fo = open(file, 'wb')
    while True:
        chunk = fi.read(8192)
        if not chunk:
            break
        fo.write(chunk.replace('\x00', ''))

    fi.close()
    fo.close()

def generate_schema(schema, table, filepath, delimiter=','):
    '''Generates a table DDL statement based on the file'''

    infile = open(filepath, 'rb')
    dr = csv.DictReader(infile, delimiter=delimiter)

    fields = {}
    for row in dr:
        for field in dr.fieldnames:
            if not fields.has_key(field):
                fields[field] = columndef(field)
            fields[field].sample_value(row[field])

    body = []

    for name in dr.fieldnames:
        body.append(fields[name].print_sql())

    schema = 'CREATE TABLE [%s].[%s] (\n\t%s\n)' % (schema, table, "\n\t, ".join(body))
    logger.debug(schema)
    return schema

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

    def print_sql(self):
        '''Prints the DDL for defining the column'''

        if self.type == '':
            self.type = 'text'

        sql = "[" + self.name + "]"
        if self.type == 'int' and self.max_value > 32768:
            sql += ' INT'
        elif self.type =='int':
            sql += ' SMALLINT'
        elif self.type == 'float':
            sql += ' FLOAT'
        elif self.type == 'date':
            sql += ' DATETIME'
        elif self.type == 'text':
            sql += ' VARCHAR('
            if self.length < 50:
                sql += '50)'
            elif self.length < 100:
                sql += '100)'
            elif  self.length < 200:
                sql += '200)'
            elif  self.length < 500:
                sql += '500)'
            elif  self.length < 1000:
                sql += '1000)'
            else:
                sql += 'MAX)'
        if self.nullable:
            sql += ' NULL'
        else:
            sql += ' NOT NULL'

        return sql

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
        try:
            time.strptime(s, "%m/%d/%y")
            return True
        except ValueError:
            return False






from email.utils import formatdate

from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import smtplib
import mimetypes
import types

def email_file(email_from, email_to, email_cc, email_subject, email_body, filepath):
    if type(email_to) is not types.ListType and email_to != None:
        email_to = [email_to]
    if type(email_cc) is not types.ListType and email_cc != None:
        email_cc = [email_cc]

    HOST = "smtp.va.gov"
    msg = MIMEMultipart()
    msg["From"] = email_from
    msg["To"] = ", ".join(email_to)
    if len(email_cc) > 0:
        msg["CC"] = ", ".join(email_cc)
    msg["Subject"] = email_subject
    msg['Date']    = formatdate(localtime=True)

    body = MIMEText(email_body)
    msg.attach(body)

    # attach a file

    msg.attach(_generate_attachment(filepath))

    server = smtplib.SMTP(HOST)

    try:
        failed = server.sendmail(email_from, email_to + email_cc, msg.as_string())
        print failed
    except Exception, e:
        logger.error("Unable to send email. Error: %s" % str(e))
    finally:
        server.close()

def _generate_attachment(filepath):
    ctype, encoding = mimetypes.guess_type(filepath)
    if ctype is None or encoding is not None:
        # No guess could be made, or the file is encoded (compressed), so
        # use a generic bag-of-bits type.
        ctype = 'application/octet-stream'
    maintype, subtype = ctype.split('/', 1)
    if maintype == 'text':
        fp = open(filepath)
        # Note: we should handle calculating the charset
        msg = MIMEText(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == 'image':
        fp = open(filepath, 'rb')
        msg = MIMEImage(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == 'audio':
        fp = open(filepath, 'rb')
        msg = MIMEAudio(fp.read(), _subtype=subtype)
        fp.close()
    else:
        fp = open(filepath, 'rb')
        msg = MIMEBase(maintype, subtype)
        msg.set_payload(fp.read())
        fp.close()
        # Encode the payload using Base64
        encoders.encode_base64(msg)
        # Set the filename parameter
    msg.add_header('Content-Disposition', 'attachment', filename=path.basename(filepath))

    return msg