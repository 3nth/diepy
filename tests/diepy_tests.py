from nose.tools import *
import diepy
from diepy import core

import logging

logger = logging.getLogger()


def setup():
    print "SETUP!"


def teardown():
    print "TEAR IT DOWN!"


def test_basic():
    print "I Ran!"


def test():
    pass


def test_init_database():
    db = core.Database('test')


def test_basic_import():
    datafile = 'basic.csv'
    db = core.Database('test')
    rows = db.import_file(datafile, 'basic')
    logger.info("Imported %s rows from %s" % (rows, datafile))


def test_basic_export():
    datafile = 'export.csv'
    db = core.Database('test')
    db.export_table('basic', datafile)


def test_basic_export_gz():
    datafile = 'export.csv.gz'
    db = core.Database('test')
    db.export_table('basic', datafile)


def test_basic_export_tab():
    datafile = 'export.tab'
    db = core.Database('test')
    db.export_table('basic', datafile)


def test_basic_export_xlsx():
    datafile = 'export.xlsx'
    db = core.Database('test')
    db.export_table('basic', datafile)
