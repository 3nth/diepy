import logging

__version__ = '0.5.1'

logger = logging.getLogger(__name__)
handler = logging.NullHandler()
logger.addHandler(handler)
