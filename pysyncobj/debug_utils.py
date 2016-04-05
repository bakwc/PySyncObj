import sys
from traceback import print_exc


def _getHeader(messageType, frame):
    return "[%s] (%s, %d):" % (messageType, frame.f_code.co_filename, frame.f_lineno)


def _printLogMessage(messageType, msg, args):
    header = _getHeader(messageType, sys._getframe(2))
    if args:
        print header, msg, args
    else:
        print header, msg


def LOG_CURRENT_EXCEPTION():
    print _getHeader("EXCEPTION", sys._getframe(1))
    print_exc()


def LOG_WARNING(msg, *args):
    _printLogMessage("WARNING", msg, args)


def LOG_DEBUG(msg, *args):
    _printLogMessage("DEBUG", msg, args)
