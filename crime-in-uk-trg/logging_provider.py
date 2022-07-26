import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s][%(levelname)s]: %(message)s'
)


def get_logger(name: str = None):
    """
    Build and return logger object.
    Args:
        name: Optional. Name of the logger.

    Returns:
        Logger object.
    """
    return logging.getLogger(name)
