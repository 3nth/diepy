import logging

__version__ = '0.4.1'

logger = logging.getLogger(__name__)
handler = logging.NullHandler()
logger.addHandler(handler)
