import sys

if sys.version_info >= (3,0):
    from .pysyncobj3 import SyncObj, SyncObjException, SyncObjConf, replicated, replicated_sync,\
        FAIL_REASON, _COMMAND_TYPE, createJournal, HAS_CRYPTO
else:
    from syncobj import SyncObj, SyncObjException, SyncObjConf, replicated, replicated_sync,\
        FAIL_REASON, _COMMAND_TYPE, createJournal, HAS_CRYPTO
