import logging

logger = logging.getLogger(__name__)
handler = logging.NullHandler()
logger.addHandler(handler)
    