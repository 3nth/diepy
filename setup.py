try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'database import export utility',
    'author': 'Derek Flenniken',
    'url': 'https://github.com/3nth/diepy',
    'download_url': 'https://github.com/3nth/diepy',
    'author_email': 'derek.flenniken@ucsf.edu',
    'version': '0.1',
    'install_requires': ['nose', 'json'],
    'packages': ['diepy'],
    'scripts': ['diepy/cli.py'],
    'name': 'diepy'
}

setup(**config)