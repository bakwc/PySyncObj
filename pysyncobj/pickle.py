import sys
if sys.version_info >= (3, 0):
    import pickle
else:
    try:
        import cPickle as pickle
    except ImportError:
        import pickle

__protocol = 2


def load(file):
    return pickle.load(file)


def loads(data):
    return pickle.loads(data)


def dump(obj, file, protocol=None):
    pickle.dump(obj, file, __protocol)


def dumps(obj, protocol=None):
    return pickle.dumps(obj, __protocol)
