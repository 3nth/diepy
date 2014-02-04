import logging

__version__ = '0.2.1'

logger = logging.getLogger(__name__)
handler = logging.NullHandler()
logger.addHandler(handler)
