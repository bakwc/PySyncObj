import threading
import weakref
import time
import socket
import os
from .syncobj import SyncObjConsumer, replicated


class ReplCounter(SyncObjConsumer):
    def __init__(self):
        super(ReplCounter, self).__init__()
        self.__counter = 0

    @replicated
    def set(self, newValue):
        self.__counter = newValue
        return self.__counter

    @replicated
    def add(self, value):
        self.__counter += value
        return self.__counter

    @replicated
    def sub(self, value):
        self.__counter -= value
        return self.__counter

    @replicated
    def inc(self):
        self.__counter += 1
        return self.__counter

    def get(self):
        return self.__counter


class ReplList(SyncObjConsumer):
    def __init__(self):
        super(ReplList, self).__init__()
        self.__data = []

    @replicated
    def reset(self, newData):
        assert isinstance(newData, list)
        self.__data = newData

    @replicated
    def set(self, position, newValue):
        self.__data[position] = newValue

    @replicated
    def append(self, item):
        self.__data.append(item)

    @replicated
    def extend(self, other):
        self.__data.extend(other)

    @replicated
    def insert(self, position, element):
        self.__data.insert(position, element)

    @replicated
    def remove(self, element):
        self.__data.remove(element)

    @replicated
    def pop(self, position=None):
        return self.__data.pop(position)

    @replicated
    def sort(self, reverse=False):
        self.__data.sort(reverse=reverse)

    def index(self, element):
        return self.__data.index(element)

    def count(self, element):
        return self.__data.count(element)

    def get(self, position):
        return self.__data[position]

    def __getitem__(self, position):
        return self.__data[position]

    def __len__(self):
        return len(self.__data)

    def rawData(self):
        return self.__data


class ReplDict(SyncObjConsumer):
    def __init__(self):
        super(ReplDict, self).__init__()
        self.__data = {}

    @replicated
    def reset(self, newData):
        assert isinstance(newData, dict)
        self.__data = newData

    @replicated
    def __setitem__(self, key, value):
        self.__data[key] = value

    @replicated
    def set(self, key, value):
        self.__data[key] = value

    @replicated
    def setdefault(self, key, default):
        return self.__data.setdefault(key, default)

    @replicated
    def update(self, other):
        self.__data.update(other)

    @replicated
    def pop(self, key, default=None):
        return self.__data.pop(key, default)

    @replicated
    def clear(self):
        self.__data.clear()

    def __getitem__(self, item):
        return self.__data[item]

    def get(self, key, default=None):
        return self.__data.get(key, default)

    def __len__(self):
        return len(self.__data)

    def __contains__(self, item):
        return item in self.__data

    def keys(self):
        return self.__data.keys()

    def values(self):
        return self.__data.values()

    def items(self):
        return self.__data.items()

    def rawData(self):
        return self.__data


class ReplSet(SyncObjConsumer):
    def __init__(self):
        super(ReplSet, self).__init__()
        self.__data = set()

    @replicated
    def reset(self, newData):
        assert isinstance(newData, set)
        self.__data = newData

    @replicated
    def add(self, item):
        self.__data.add(item)

    @replicated
    def remove(self, item):
        self.__data.remove(item)

    @replicated
    def discard(self, item):
        self.__data.discard(item)

    @replicated
    def pop(self):
        return self.__data.pop()

    @replicated
    def clear(self):
        self.__data.clear()

    @replicated
    def update(self, other):
        self.__data.update(other)

    def rawData(self):
        return self.__data

    def __len__(self):
        return len(self.__data)

    def __contains__(self, item):
        return item in self.__data


class _ReplLockManagerImpl(SyncObjConsumer):
    def __init__(self, autoUnlockTime):
        super(_ReplLockManagerImpl, self).__init__()
        self.__locks = {}
        self.__autoUnlockTime = autoUnlockTime

    @replicated
    def acquire(self, lockPath, clientID, currentTime):
        existingLock = self.__locks.get(lockPath, None)
        # Auto-unlock old lock
        if existingLock is not None:
            if currentTime - existingLock[1] > self.__autoUnlockTime:
                existingLock = None
        # Acquire lock if possible
        if existingLock is None or existingLock[0] == clientID:
            self.__locks[lockPath] = (clientID, currentTime)
            return True
        # Lock already acquired by someone else
        return False

    @replicated
    def prolongate(self, clientID, currentTime):
        for lockPath in self.__locks.keys():
            lockClientID, lockTime = self.__locks[lockPath]

            if currentTime - lockTime > self.__autoUnlockTime:
                del self.__locks[lockPath]
                continue

            if lockClientID == clientID:
                self.__locks[lockPath] = (clientID, currentTime)

    @replicated
    def release(self, lockPath, clientID):
        existingLock = self.__locks.get(lockPath, None)
        if existingLock is not None and existingLock[0] == clientID:
            del self.__locks[lockPath]

    def isAcquired(self, lockPath, clientID, currentTime):
        existingLock = self.__locks.get(lockPath, None)
        if existingLock is not None:
            if existingLock[0] == clientID:
                if currentTime - existingLock[1] < self.__autoUnlockTime:
                    return True
        return False


class ReplLockManager(object):

    def __init__(self, autoUnlockTime, selfID = None):
        self.__lockImpl = _ReplLockManagerImpl(autoUnlockTime)
        if selfID is None:
            selfID = '%s:%d:%d' % (socket.gethostname(), os.getpid(), id(self))
        self.__selfID = selfID
        self.__autoUnlockTime = autoUnlockTime
        self.__mainThread = threading.current_thread()
        self.__initialised = threading.Event()
        self.__destroying = False
        self.__lastProlongateTime = 0
        self.__thread = threading.Thread(target=ReplLockManager._autoAcquireThread, args=(weakref.proxy(self),))
        self.__thread.start()
        while not self.__initialised.is_set():
            pass

    def _consumer(self):
        return self.__lockImpl

    def destroy(self):
        self.__destroying = True

    def _autoAcquireThread(self):
        self.__initialised.set()
        try:
            while True:
                if not self.__mainThread.is_alive():
                    break
                if self.__destroying:
                    break
                time.sleep(0.1)
                if time.time() - self.__lastProlongateTime < float(self.__autoUnlockTime) / 4.0:
                    continue
                syncObj = self.__lockImpl._syncObj
                if syncObj is None:
                    continue
                if syncObj._getLeader() is not None:
                    self.__lastProlongateTime = time.time()
                    self.__lockImpl.prolongate(self.__selfID, time.time())
        except ReferenceError:
            pass

    def tryAcquire(self, path, callback=None, sync=False, timeout=None):
        return self.__lockImpl.acquire(path, self.__selfID, time.time(), callback=callback, sync=sync, timeout=timeout)

    def isAcquired(self, path):
        return self.__lockImpl.isAcquired(path, self.__selfID, time.time())

    def release(self, path, callback=None, sync=False, timeout=None):
        self.__lockImpl.release(path, self.__selfID, callback=callback, sync=sync, timeout=timeout)
