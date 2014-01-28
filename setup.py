from ez_setup import use_setuptools
use_setuptools()
import diepy

from setuptools import setup, find_packages

config = {
    'description': 'database import export utility',
    'author': 'Derek Flenniken',
    'url': 'https://github.com/3nth/diepy',
    'download_url': 'https://github.com/3nth/diepy',
    'author_email': 'derek.flenniken@ucsf.edu',
    'version': diepy.__version__,
    'install_requires': ['nose', 'SQLAlchemy', 'python-dateutil'],
    'packages': find_packages(),
    'entry_points': {
        'console_scripts': [
            'diepy = diepy.cli:run'
        ]
    },
    'name': 'diepy'
}

setup(**config)