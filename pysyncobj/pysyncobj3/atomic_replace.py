#
#  WARNING: this is generated file, use generate.sh to update it.
#
import os
import sys
import ctypes

if hasattr(ctypes, 'windll'):     # pragma: no cover
    CreateTransaction = ctypes.windll.ktmw32.CreateTransaction
    CommitTransaction = ctypes.windll.ktmw32.CommitTransaction
    MoveFileTransacted = ctypes.windll.kernel32.MoveFileTransactedW
    CloseHandle = ctypes.windll.kernel32.CloseHandle

    MOVEFILE_REPLACE_EXISTING = 0x1
    MOVEFILE_WRITE_THROUGH = 0x8

    def atomicReplace(oldPath, newPath):
        if not isinstance(oldPath, str):
            oldPath = str(oldPath, sys.getfilesystemencoding())
        if not isinstance(newPath, str):
            newPath = str(newPath, sys.getfilesystemencoding())
        ta = CreateTransaction(None, 0, 0, 0, 0, 1000, 'atomic_replace')
        if ta == -1:
            return False
        res = MoveFileTransacted(oldPath, newPath, None, None, MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH, ta)
        if not res:
            CloseHandle(ta)
            return False
        res = CommitTransaction(ta)
        CloseHandle(ta)
        return bool(res)
else:
    atomicReplace = os.rename
