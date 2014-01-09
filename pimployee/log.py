import os
import sys
import logging

# use the logging.Logger object directly rather than logging.getLogger
# in order to ensure complete separation of ours and the user's
# logging machinery.

logger = None
pilogger = None

def setup_log(fileno):
    """Logs to a file in our boss. For informing ourselves."""
    
    global logger
    logger = logging.Logger('log')
    handler = logging.StreamHandler(os.fdopen(fileno, 'a', 0))
    handler.setFormatter(logging.Formatter("[%(asctime)s] - [%(levelname)s] - %(message)s", datefmt=None))
    logger.addHandler(handler)

def setup_pilog(fileno):
    """Logs to special pilog entry in job output. For informing users."""
    
    global pilogger
    pilogger = logging.Logger('pilog')
    handler = logging.StreamHandler(os.fdopen(fileno, 'w', 0))
    handler.setFormatter(logging.Formatter("[%(asctime)s] - [%(levelname)s] - %(message)s", datefmt=None))
    pilogger.addHandler(handler)

def _log_excepthook(exc_type, exc_obj, exc_tb):
    formatted_exception = logging.Formatter().formatException((exc_type, exc_obj, exc_tb))
    if logger:
        logger.info('employee had uncaught exception in main thread\n: %s' % formatted_exception)

def setup_excepthook():
    sys.excepthook = _log_excepthook

