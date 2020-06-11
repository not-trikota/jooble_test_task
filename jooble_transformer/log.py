import logging


FORMAT = logging.Formatter('%(asctime)-15s %(name)s: %(levelname)s : %(message)s')


def get_logger(name, filename=None, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    stream = logging.StreamHandler()
    stream.setFormatter(FORMAT)
    logger.addHandler(stream)

    if filename is not None:
        fh = logging.FileHandler(filename)
        fh.setFormatter(FORMAT)
        logger.addHandler(fh)
    logger.propagate = False
    return logger
