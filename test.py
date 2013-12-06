#!/usr/bin/env python
from diepy import core
import logging

logging.basicConfig(level=logging.DEBUG)
db = core.Database('test')
rows = db.import_file('/Users/vhasfcflennd/projects/diepy/tests/basic.csv', 'basic')
print str(rows)
db.export_table('basic', 'basic.csv')
