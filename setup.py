#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import diepy

from setuptools import setup, find_packages

config = {
    'name': 'diepy',
    'description': 'database import export utility',
    'version': diepy.__version__,
    'license': 'MIT License',
    'author': 'Derek Flenniken',
    'author_email': 'derek.flenniken@ucsf.edu',
    'url': 'https://github.com/3nth/diepy',
    'download_url': 'https://github.com/3nth/diepy',
    'packages': ['diepy'],
    'package_dir': {'diepy': 'diepy'},
    'platforms': 'any',
    'tests_require': ['nose'],
    'install_requires': ['SQLAlchemy', 'python-dateutil', 'cliff'],
    'scripts': [],
    'entry_points': {
        'console_scripts': [
            'diepy = diepy.cli:main'
        ],
        'diepy': [
            'import = diepy.cli:Import',
            'export = diepy.cli:Export'
        ]
    },
    'zip_safe': False,
    'classifiers': [
        'Programming Language :: Python',
        'Development Status :: 4 - Beta',
        'Natural Language :: English',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ]
}

setup(**config)