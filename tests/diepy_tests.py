from nose.tools import *
import diepy
from diepy import core2

import logging

logger = logging.getLogger()

def setup():
    print "SETUP!"

def teardown():
    print "TEAR IT DOWN!"

def test_basic():
    print "I Ran!"

def test_init_dbserver():
    db = core2.dbserver('test')
    
def test_basic_import():
    datafile = 'basic.csv'
    db = core2.dbserver('test')
    rows = db.import_file(datafile, 'basic')
    logger.info("Imported %s rows from %s" % (rows, datafile))

def test_basic_export():
    return
    datafile = 'export.csv'
    db = core2.dbserver('test')
    db.export_table('basic', datafile)
