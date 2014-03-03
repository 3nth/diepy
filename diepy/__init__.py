import logging

__version__ = '0.6.0'

logger = logging.getLogger(__name__)
handler = logging.NullHandler()
logger.addHandler(handler)
