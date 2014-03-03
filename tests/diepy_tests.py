import os
from nose.tools import *
import diepy
from diepy import core

import logging

logger = logging.getLogger()

here = os.path.abspath(os.path.dirname(__file__))
db = core.Database('test', config=os.path.join(here, 'diepy.ini'))

def setup():
    print "SETUP!"


def teardown():
    print "TEAR IT DOWN!"

def test_basic_import():
    datafile = os.path.join(here, 'basic.csv')
    rows = db.import_file(datafile, 'basic')
    logger.info("Imported %s rows from %s" % (rows, datafile))


def test_basic_export():
    datafile = os.path.join(here,'export.csv')
    db.export_table('basic', datafile)


def test_basic_export_gz():
    datafile = os.path.join(here,'export.csv.gz')
    db.export_table('basic', datafile)


def test_basic_export_tab():
    datafile = os.path.join(here, 'export.tab')
    db.export_table('basic', datafile)


def test_basic_export_xlsx():
    datafile = os.path.join(here, 'export.xlsx')
    db.export_table('basic', datafile)
