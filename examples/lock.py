#!/usr/bin/env python
from __future__ import print_function

import sys
import threading
import weakref
import time
sys.path.append("../")
from pysyncobj import SyncObj, replicated


class LockImpl(SyncObj):
    def __init__(self, selfAddress, partnerAddrs, autoUnlockTime):
        super(LockImpl, self).__init__(selfAddress, partnerAddrs)
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
    def ping(self, clientID, currentTime):
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


class Lock(object):
    def __init__(self, selfAddress, partnerAddrs, autoUnlockTime):
        self.__lockImpl = LockImpl(selfAddress, partnerAddrs, autoUnlockTime)
        self.__selfID = selfAddress
        self.__autoUnlockTime = autoUnlockTime
        self.__mainThread = threading.current_thread()
        self.__initialised = threading.Event()
        self.__thread = threading.Thread(target=Lock._autoAcquireThread, args=(weakref.proxy(self),))
        self.__thread.start()
        while not self.__initialised.is_set():
            pass

    def _autoAcquireThread(self):
        self.__initialised.set()
        try:
            while True:
                if not self.__mainThread.is_alive():
                    break
                time.sleep(float(self.__autoUnlockTime) / 4.0)
                if self.__lockImpl._getLeader() is not None:
                    self.__lockImpl.ping(self.__selfID, time.time())
        except ReferenceError:
            pass

    def tryAcquireLock(self, path):
        self.__lockImpl.acquire(path, self.__selfID, time.time())

    def isAcquired(self, path):
        return self.__lockImpl.isAcquired(path, self.__selfID, time.time())

    def release(self, path):
        self.__lockImpl.release(path, self.__selfID)

    def printStatus(self):
        self.__lockImpl._printStatus()


def printHelp():
    print('')
    print('        Available commands:')
    print('')
    print('help                 print this help')
    print('check lockPath       check if lock with lockPath path is ackquired or released')
    print('acquire lockPath     try to ackquire lock with lockPath')
    print('release lockPath     try to release lock with lockPath')
    print('')
    print('')


def main():
    if len(sys.argv) < 3:
        print('Usage: %s selfHost:port partner1Host:port partner2Host:port ...' % sys.argv[0])
        sys.exit(-1)

    selfAddr = sys.argv[1]
    partners = sys.argv[2:]

    lock = Lock(selfAddr, partners, 10.0)

    def get_input(v):
        if sys.version_info >= (3, 0):
            return input(v)
        else:
            return raw_input(v)

    printHelp()
    while True:
        cmd = get_input(">> ").split()
        if not cmd:
            continue
        elif cmd[0] == 'help':
            printHelp()
        elif cmd[0] == 'check':
            print('acquired' if lock.isAcquired(cmd[1]) else 'released')
        elif cmd[0] == 'acquire':
            lock.tryAcquireLock(cmd[1])
            time.sleep(1.5)
            print('acquired' if lock.isAcquired(cmd[1]) else 'failed')
        elif cmd[0] == 'release':
            lock.release(cmd[1])
            time.sleep(1.5)
            print('acquired' if lock.isAcquired(cmd[1]) else 'released')


if __name__ == '__main__':
    main()
