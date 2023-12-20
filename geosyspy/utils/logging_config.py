# geosyspy/logging_config.py

import logging


def configure_logging(level=logging.INFO, handlers=None):
    """
    Configure logs for geosyspy.
    Args:
        level (int): log level
        handlers ([logging.handler]): logger handlers
    """
    logger = logging.getLogger('geosyspy')
    if handlers is None:
        handlers = [logging.StreamHandler()]  # By default, use a console handler

    for handler in handlers:
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ))
        logger.addHandler(handler)

