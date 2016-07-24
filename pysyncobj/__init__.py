import sys

if sys.version_info >= (3,0):
    from .pysyncobj3 import SyncObj, SyncObjConf, replicated, FAIL_REASON, _COMMAND_TYPE
else:
    from syncobj import SyncObj, SyncObjConf, replicated, FAIL_REASON, _COMMAND_TYPE
