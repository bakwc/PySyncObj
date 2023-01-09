from __future__ import print_function
import os
import time
import pytest
import random
import threading
import sys
import pysyncobj.pickle as pickle
import pysyncobj.dns_resolver as dns_resolver
import platform

if sys.version_info >= (3, 0):
    xrange = range
from functools import partial
import functools
import struct
import logging
from pysyncobj import SyncObj, SyncObjConf, replicated, FAIL_REASON, _COMMAND_TYPE, \
    createJournal, HAS_CRYPTO, replicated_sync, SyncObjException, SyncObjConsumer, _RAFT_STATE
from pysyncobj.syncobj_admin import executeAdminCommand
from pysyncobj.batteries import ReplCounter, ReplList, ReplDict, ReplSet, ReplLockManager, ReplQueue, ReplPriorityQueue
from pysyncobj.node import TCPNode
from collections import defaultdict

logging.basicConfig(format=u'[%(asctime)s %(filename)s:%(lineno)d %(levelname)s]  %(message)s', level=logging.DEBUG)

_bchr = functools.partial(struct.pack, 'B')


class TEST_TYPE:
    DEFAULT = 0
    COMPACTION_1 = 1
    COMPACTION_2 = 2
    RAND_1 = 3
    JOURNAL_1 = 4
    AUTO_TICK_1 = 5
    WAIT_BIND = 6
    LARGE_COMMAND = 7


class TestObj(SyncObj):

    def __init__(self, selfNodeAddr, otherNodeAddrs,
                 testType=TEST_TYPE.DEFAULT,
                 compactionMinEntries=0,
                 dumpFile=None,
                 journalFile=None,
                 password=None,
                 dynamicMembershipChange=False,
                 useFork=True,
                 testBindAddr=False,
                 consumers=None,
                 onStateChanged=None,
                 leaderFallbackTimeout=None):

        cfg = SyncObjConf(autoTick=False, appendEntriesUseBatch=False)
        cfg.appendEntriesPeriod = 0.1
        cfg.raftMinTimeout = 0.5
        cfg.raftMaxTimeout = 1.0
        cfg.dynamicMembershipChange = dynamicMembershipChange
        cfg.onStateChanged = onStateChanged
        if leaderFallbackTimeout is not None:
            cfg.leaderFallbackTimeout = leaderFallbackTimeout

        if testBindAddr:
            cfg.bindAddress = selfNodeAddr

        if dumpFile is not None:
            cfg.fullDumpFile = dumpFile

        if password is not None:
            cfg.password = password

        cfg.useFork = useFork

        if testType == TEST_TYPE.COMPACTION_1:
            cfg.logCompactionMinEntries = compactionMinEntries
            cfg.logCompactionMinTime = 0.1
            cfg.appendEntriesUseBatch = True

        if testType == TEST_TYPE.COMPACTION_2:
            cfg.logCompactionMinEntries = 99999
            cfg.logCompactionMinTime = 99999
            cfg.fullDumpFile = dumpFile

        if testType == TEST_TYPE.LARGE_COMMAND:
            cfg.connectionTimeout = 15.0
            cfg.logCompactionMinEntries = 99999
            cfg.logCompactionMinTime = 99999
            cfg.fullDumpFile = dumpFile
            cfg.raftMinTimeout = 1.5
            cfg.raftMaxTimeout = 2.5
        # cfg.appendEntriesBatchSizeBytes = 2 ** 13

        if testType == TEST_TYPE.RAND_1:
            cfg.autoTickPeriod = 0.05
            cfg.appendEntriesPeriod = 0.02
            cfg.raftMinTimeout = 0.1
            cfg.raftMaxTimeout = 0.2
            cfg.logCompactionMinTime = 9999999
            cfg.logCompactionMinEntries = 9999999
            cfg.journalFile = journalFile

        if testType == TEST_TYPE.JOURNAL_1:
            cfg.logCompactionMinTime = 999999
            cfg.logCompactionMinEntries = 999999
            cfg.fullDumpFile = dumpFile
            cfg.journalFile = journalFile

        if testType == TEST_TYPE.AUTO_TICK_1:
            cfg.autoTick = True
            cfg.pollerType = 'select'

        if testType == TEST_TYPE.WAIT_BIND:
            cfg.maxBindRetries = 1
            cfg.autoTick = True

        super(TestObj, self).__init__(selfNodeAddr, otherNodeAddrs, cfg, consumers)
        self.__counter = 0
        self.__data = {}

        if testType == TEST_TYPE.RAND_1:
            self._SyncObj__transport._send_random_sleep_duration = 0.03

    @replicated
    def addValue(self, value):
        self.__counter += value
        return self.__counter

    @replicated
    def addKeyValue(self, key, value):
        self.__data[key] = value

    @replicated_sync
    def addValueSync(self, value):
        self.__counter += value
        return self.__counter

    @replicated
    def testMethod(self):
        self.__data['testKey'] = 'valueVer1'

    @replicated(ver=1)
    def testMethod(self):
        self.__data['testKey'] = 'valueVer2'

    def getCounter(self):
        return self.__counter

    def getValue(self, key):
        return self.__data.get(key, None)

    def dumpKeys(self):
        print('keys:', sorted(self.__data.keys()))


def singleTickFunc(o, timeToTick, interval, stopFunc):
    currTime = time.time()
    finishTime = currTime + timeToTick
    while time.time() < finishTime:
        o._onTick(interval)
        if stopFunc is not None:
            if stopFunc():
                break


def utilityTickFunc(args, currRes, key):
    currRes[key] = executeAdminCommand(args)


def doSyncObjAdminTicks(objects, arguments, timeToTick, currRes, interval=0.05, stopFunc=None):
    objThreads = []
    utilityThreads = []
    for o in objects:
        t1 = threading.Thread(target=singleTickFunc, args=(o, timeToTick, interval, stopFunc))
        t1.start()
        objThreads.append(t1)
        if arguments.get(o) is not None:
            t2 = threading.Thread(target=utilityTickFunc, args=(arguments[o], currRes, o))
            t2.start()
            utilityThreads.append(t2)
    for t in objThreads:
        t.join()
    for t in utilityThreads:
        t.join()


def doTicks(objects, timeToTick, interval=0.05, stopFunc=None):
    threads = []
    for o in objects:
        t = threading.Thread(target=singleTickFunc, args=(o, timeToTick, interval, stopFunc))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()


def doAutoTicks(interval=0.05, stopFunc=None):
    deadline = time.time() + interval
    while not stopFunc():
        time.sleep(0.02)
        t2 = time.time()
        if t2 >= deadline:
            break


_g_nextAddress = 6000 + 60 * (int(time.time()) % 600)


def getNextAddr(ipv6=False, isLocalhost=False):
    global _g_nextAddress
    _g_nextAddress += 1
    if ipv6:
        return '::1:%d' % _g_nextAddress
    if isLocalhost:
        return 'localhost:%d' % _g_nextAddress
    return '127.0.0.1:%d' % _g_nextAddress


_g_nextDumpFile = 1
_g_nextJournalFile = 1


def getNextDumpFile():
    global _g_nextDumpFile
    fname = 'dump%d.bin' % _g_nextDumpFile
    _g_nextDumpFile += 1
    return fname


def getNextJournalFile():
    global _g_nextJournalFile
    fname = 'journal%d.bin' % _g_nextJournalFile
    _g_nextJournalFile += 1
    return fname


def test_syncTwoObjects():
    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]])
    o2 = TestObj(a[1], [a[0]])
    objs = [o1, o2]

    assert not o1._isReady()
    assert not o2._isReady()

    doTicks(objs, 10.0, stopFunc=lambda: o1._isReady() and o2._isReady())

    o1.waitBinded()
    o2.waitBinded()

    o1._printStatus()

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()
    assert o1._isReady()
    assert o2._isReady()

    o1.addValue(150)
    o2.addValue(200)

    doTicks(objs, 10.0, stopFunc=lambda: o1.getCounter() == 350 and o2.getCounter() == 350)

    assert o1._isReady()
    assert o2._isReady()

    assert o1.getCounter() == 350
    assert o2.getCounter() == 350

    o1._destroy()
    o2._destroy()


def test_hasQuorum():
    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]])
    o2 = TestObj(a[1], [a[0]])
    objs = [o1, o2]

    doTicks(objs, 10.0, stopFunc=lambda: o1._isReady() and o2._isReady())

    o1.waitBinded()
    o2.waitBinded()

    o1._printStatus()
    assert o1.hasQuorum

    # Stop the second node in the cluster
    o2._destroy()
    doTicks(objs, 10.0, stopFunc=lambda: not o1.hasQuorum)

    assert not o1.hasQuorum

    o1._destroy()


def test_singleObject():
    random.seed(42)

    a = [getNextAddr(), ]

    o1 = TestObj(a[0], [])
    objs = [o1, ]

    assert not o1._isReady()

    doTicks(objs, 3.0, stopFunc=lambda: o1._isReady())

    o1._printStatus()

    assert o1._getLeader().address in a
    assert o1._isReady()

    o1.addValue(150)
    o1.addValue(200)

    doTicks(objs, 3.0, stopFunc=lambda: o1.getCounter() == 350)

    assert o1._isReady()

    assert o1.getCounter() == 350

    o1._destroy()


def test_syncThreeObjectsLeaderFail():
    random.seed(12)

    a = [getNextAddr(), getNextAddr(), getNextAddr()]

    states = defaultdict(list)

    o1 = TestObj(a[0], [a[1], a[2]], testBindAddr=True, onStateChanged=lambda old, new: states[a[0]].append(new))
    o2 = TestObj(a[1], [a[2], a[0]], testBindAddr=True, onStateChanged=lambda old, new: states[a[1]].append(new))
    o3 = TestObj(a[2], [a[0], a[1]], testBindAddr=True, onStateChanged=lambda old, new: states[a[2]].append(new))
    objs = [o1, o2, o3]

    assert not o1._isReady()
    assert not o2._isReady()
    assert not o3._isReady()

    doTicks(objs, 10.0, stopFunc=lambda: o1._isReady() and o2._isReady() and o3._isReady())

    assert o1._isReady()
    assert o2._isReady()
    assert o3._isReady()

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()
    assert o1._getLeader() == o3._getLeader()

    assert _RAFT_STATE.LEADER in states[o1._getLeader().address]

    o1.addValue(150)
    o2.addValue(200)

    doTicks(objs, 10.0, stopFunc=lambda: o3.getCounter() == 350)

    assert o3.getCounter() == 350

    prevLeader = o1._getLeader()

    newObjs = [o for o in objs if o._SyncObj__selfNode != prevLeader]

    assert len(newObjs) == 2

    doTicks(newObjs, 10.0, stopFunc=lambda: newObjs[0]._getLeader() != prevLeader and \
                                            newObjs[0]._getLeader() is not None and \
                                            newObjs[0]._getLeader().address in a and \
                                            newObjs[0]._getLeader() == newObjs[1]._getLeader())

    assert newObjs[0]._getLeader() != prevLeader
    assert newObjs[0]._getLeader().address in a
    assert newObjs[0]._getLeader() == newObjs[1]._getLeader()

    assert _RAFT_STATE.LEADER in states[newObjs[0]._getLeader().address]

    newObjs[1].addValue(50)

    doTicks(newObjs, 10, stopFunc=lambda: newObjs[0].getCounter() == 400)

    assert newObjs[0].getCounter() == 400

    doTicks(objs, 10.0, stopFunc=lambda: sum([int(o.getCounter() == 400) for o in objs]) == len(objs))

    for o in objs:
        assert o.getCounter() == 400

    o1._destroy()
    o2._destroy()
    o3._destroy()


def test_manyActionsLogCompaction():
    random.seed(42)

    a = [getNextAddr(), getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1], a[2]], TEST_TYPE.COMPACTION_1, compactionMinEntries=100)
    o2 = TestObj(a[1], [a[2], a[0]], TEST_TYPE.COMPACTION_1, compactionMinEntries=100)
    o3 = TestObj(a[2], [a[0], a[1]], TEST_TYPE.COMPACTION_1, compactionMinEntries=100)
    objs = [o1, o2, o3]

    assert not o1._isReady()
    assert not o2._isReady()
    assert not o3._isReady()

    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady() and o3._isReady())

    assert o1._isReady()
    assert o2._isReady()
    assert o3._isReady()

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()
    assert o1._getLeader() == o3._getLeader()

    for i in xrange(0, 500):
        o1.addValue(1)
        o2.addValue(1)

    doTicks(objs, 10, stopFunc=lambda:
        o1.getCounter() == 1000 and
        o2.getCounter() == 1000 and
        o3.getCounter() == 1000 and
        o1._getRaftLogSize() <= 100 and
        o2._getRaftLogSize() <= 100 and
        o3._getRaftLogSize() <= 100
    )

    assert o1.getCounter() == 1000
    assert o2.getCounter() == 1000
    assert o3.getCounter() == 1000

    assert o1._getRaftLogSize() <= 100
    assert o2._getRaftLogSize() <= 100
    assert o3._getRaftLogSize() <= 100

    newObjs = [o1, o2]
    doTicks(newObjs, 10, stopFunc=lambda: o3._getLeader() is None)

    for i in xrange(0, 500):
        o1.addValue(1)
        o2.addValue(1)

    doTicks(newObjs, 10, stopFunc=lambda:
        o1.getCounter() == 2000 and
        o2.getCounter() == 2000 and
        o1._getRaftLogSize() <= 100 and
        o2._getRaftLogSize() <= 100 and
        o3._getRaftLogSize() <= 100
    )

    assert o1.getCounter() == 2000
    assert o2.getCounter() == 2000
    assert o3.getCounter() != 2000

    doTicks(objs, 10, stopFunc=lambda: o3.getCounter() == 2000)

    assert o3.getCounter() == 2000

    assert o1._getRaftLogSize() <= 100
    assert o2._getRaftLogSize() <= 100
    assert o3._getRaftLogSize() <= 100

    o1._destroy()
    o2._destroy()
    o3._destroy()


def onAddValue(res, err, info):
    assert res == 3
    assert err == FAIL_REASON.SUCCESS
    info['callback'] = True


def test_checkCallbacksSimple():
    random.seed(42)

    a = [getNextAddr(), getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1], a[2]])
    o2 = TestObj(a[1], [a[2], a[0]])
    o3 = TestObj(a[2], [a[0], a[1]])
    objs = [o1, o2, o3]

    assert not o1._isReady()
    assert not o2._isReady()
    assert not o3._isReady()

    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady() and o3._isReady())

    assert o1._isReady()
    assert o2._isReady()
    assert o3._isReady()

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()
    assert o1._getLeader() == o3._getLeader()

    callbackInfo = {
        'callback': False
    }
    o1.addValue(3, callback=partial(onAddValue, info=callbackInfo))

    doTicks(objs, 10, stopFunc=lambda: o2.getCounter() == 3 and callbackInfo['callback'] == True)

    assert o2.getCounter() == 3
    assert callbackInfo['callback'] == True

    o1._destroy()
    o2._destroy()
    o3._destroy()


def removeFiles(files):
    for f in (files):
        if os.path.isfile(f):
            for i in xrange(0, 15):
                try:
                    if os.path.isfile(f):
                        os.remove(f)
                        break
                    else:
                        break
                except:
                    time.sleep(1.0)


def checkDumpToFile(useFork):
    dumpFiles = [getNextDumpFile(), getNextDumpFile()]
    removeFiles(dumpFiles)

    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile=dumpFiles[0], useFork=useFork)
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile=dumpFiles[1], useFork=useFork)
    objs = [o1, o2]
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    o1.addValue(150)
    o2.addValue(200)

    doTicks(objs, 10, stopFunc=lambda: o1.getCounter() == 350 and o2.getCounter() == 350)

    assert o1.getCounter() == 350
    assert o2.getCounter() == 350

    o1._forceLogCompaction()
    o2._forceLogCompaction()

    doTicks(objs, 1.5)

    o1._destroy()
    o2._destroy()

    a = [getNextAddr(), getNextAddr()]
    o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile=dumpFiles[0], useFork=useFork)
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile=dumpFiles[1], useFork=useFork)
    objs = [o1, o2]
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())
    assert o1._isReady()
    assert o2._isReady()

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    assert o1.getCounter() == 350
    assert o2.getCounter() == 350

    o1._destroy()
    o2._destroy()

    removeFiles(dumpFiles)


def test_checkDumpToFile():
    if hasattr(os, 'fork'):
        checkDumpToFile(True)
    checkDumpToFile(False)


def getRandStr():
    return '%0100000x' % random.randrange(16 ** 100000)


def test_checkBigStorage():
    dumpFiles = [getNextDumpFile(), getNextDumpFile()]
    removeFiles(dumpFiles)

    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile=dumpFiles[0])
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile=dumpFiles[1])
    objs = [o1, o2]
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    # Store ~50Mb data.
    testRandStr = getRandStr()
    for i in xrange(0, 500):
        o1.addKeyValue(i, getRandStr())
    o1.addKeyValue('test', testRandStr)

    # Wait for replication.
    doTicks(objs, 60, stopFunc=lambda: o1.getValue('test') == testRandStr and \
                                       o2.getValue('test') == testRandStr)

    assert o1.getValue('test') == testRandStr

    o1._forceLogCompaction()
    o2._forceLogCompaction()

    # Wait for disk dump
    doTicks(objs, 8.0)

    o1._destroy()
    o2._destroy()

    a = [getNextAddr(), getNextAddr()]
    o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile=dumpFiles[0])
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile=dumpFiles[1])
    objs = [o1, o2]
    # Wait for disk load, election and replication
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    assert o1.getValue('test') == testRandStr
    assert o2.getValue('test') == testRandStr

    o1._destroy()
    o2._destroy()

    removeFiles(dumpFiles)

@pytest.mark.skipif(sys.platform == "win32" or platform.python_implementation() != 'CPython', reason="does not run on windows or pypy")
def test_encryptionCorrectPassword():
    assert HAS_CRYPTO

    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], password='asd')
    o2 = TestObj(a[1], [a[0]], password='asd')
    objs = [o1, o2]
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    o1.addValue(150)
    o2.addValue(200)

    doTicks(objs, 10, stopFunc=lambda: o1.getCounter() == 350 and o2.getCounter() == 350)

    assert o1.getCounter() == 350
    assert o2.getCounter() == 350

    for conn in list(o1._SyncObj__transport._connections.values()) + list(o2._SyncObj__transport._connections.values()):
        conn.disconnect()

    doTicks(objs, 10)

    o1.addValue(100)

    doTicks(objs, 10, stopFunc=lambda: o1.getCounter() == 450 and o2.getCounter() == 450)

    assert o1.getCounter() == 450
    assert o2.getCounter() == 450

    o1._destroy()
    o2._destroy()

@pytest.mark.skipif(platform.python_implementation() != 'CPython', reason="does not have crypto on pypy")
def test_encryptionWrongPassword():
    assert HAS_CRYPTO

    random.seed(12)

    a = [getNextAddr(), getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1], a[2]], password='asd')
    o2 = TestObj(a[1], [a[2], a[0]], password='asd')
    o3 = TestObj(a[2], [a[0], a[1]], password='qwe')
    objs = [o1, o2, o3]

    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    doTicks(objs, 1.0)
    assert o3._getLeader() is None

    o1._destroy()
    o2._destroy()
    o3._destroy()


def _checkSameLeader(objs):
    for obj1 in objs:
        l1 = obj1._getLeader()
        if l1 != obj1._SyncObj__selfNode:
            continue
        t1 = obj1._getTerm()
        for obj2 in objs:
            l2 = obj2._getLeader()
            if l2 != obj2._SyncObj__selfNode:
                continue
            if obj2._getTerm() != t1:
                continue
            if l2 != l1:
                obj1._printStatus()
                obj2._printStatus()
                return False
    return True


def _checkSameLeader2(objs):
    for obj1 in objs:
        l1 = obj1._getLeader()
        if l1 is None:
            continue
        t1 = obj1._getTerm()
        for obj2 in objs:
            l2 = obj2._getLeader()
            if l2 is None:
                continue
            if obj2._getTerm() != t1:
                continue
            if l2 != l1:
                obj1._printStatus()
                obj2._printStatus()
                return False
    return True


def test_randomTest1():
    journalFiles = [getNextJournalFile(), getNextJournalFile(), getNextJournalFile()]
    removeFiles(journalFiles)
    removeFiles([e + '.meta' for e in journalFiles])

    random.seed(12)

    a = [getNextAddr(), getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1], a[2]], TEST_TYPE.RAND_1, journalFile=journalFiles[0])
    o2 = TestObj(a[1], [a[2], a[0]], TEST_TYPE.RAND_1, journalFile=journalFiles[1])
    o3 = TestObj(a[2], [a[0], a[1]], TEST_TYPE.RAND_1, journalFile=journalFiles[2])
    objs = [o1, o2, o3]

    raft_commit_indices = [0, 0, 0]

    st = time.time()
    while time.time() - st < 120.0:
        doTicks(objs, random.random() * 0.3, interval=0.05)

        for i in range(3):
            new_commit_idx = objs[i]._SyncObj__raftCommitIndex
            assert new_commit_idx >= raft_commit_indices[i]
            raft_commit_indices[i] = new_commit_idx

        assert _checkSameLeader(objs)
        assert _checkSameLeader2(objs)
        for i in xrange(0, random.randint(0, 2)):
            random.choice(objs).addValue(random.randint(0, 10))
        newObjs = list(objs)
        newObjs.pop(random.randint(0, len(newObjs) - 1))
        doTicks(newObjs, random.random() * 0.3, interval=0.05)

        for i in range(3):
            new_commit_idx = objs[i]._SyncObj__raftCommitIndex
            assert new_commit_idx >= raft_commit_indices[i]
            raft_commit_indices[i] = new_commit_idx

        assert _checkSameLeader(objs)
        assert _checkSameLeader2(objs)
        for i in xrange(0, random.randint(0, 2)):
            random.choice(objs).addValue(random.randint(0, 10))

    if not (o1.getCounter() == o2.getCounter() == o3.getCounter()):
        print(time.time(), 'counters:', o1.getCounter(), o2.getCounter(), o3.getCounter())

        # disable send delays to make test finish faster
        for obj in objs:
            obj._SyncObj__transport._send_random_sleep_duration = 0.00

    st = time.time()
    while not (o1.getCounter() == o2.getCounter() == o3.getCounter()):
        doTicks(objs, 2.0, interval=0.05)
        if time.time() - st > 30:
            break

    if not (o1.getCounter() == o2.getCounter() == o3.getCounter()):
        o1._printStatus()
        o2._printStatus()
        o3._printStatus()
        print('Logs same:', o1._SyncObj__raftLog == o2._SyncObj__raftLog == o3._SyncObj__raftLog)
        print(time.time(), 'counters:', o1.getCounter(), o2.getCounter(), o3.getCounter())
        raise AssertionError('Values not equal')

    counter = o1.getCounter()
    o1._destroy()
    o2._destroy()
    o3._destroy()
    del o1
    del o2
    del o3
    time.sleep(0.1)

    o1 = TestObj(a[0], [a[1], a[2]], TEST_TYPE.RAND_1, journalFile=journalFiles[0])
    o2 = TestObj(a[1], [a[2], a[0]], TEST_TYPE.RAND_1, journalFile=journalFiles[1])
    o3 = TestObj(a[2], [a[0], a[1]], TEST_TYPE.RAND_1, journalFile=journalFiles[2])
    objs = [o1, o2, o3]

    st = time.time()
    while not (o1.getCounter() == o2.getCounter() == o3.getCounter() == counter):
        doTicks(objs, 2.0, interval=0.05)
        if time.time() - st > 30:
            break

    if not (o1.getCounter() == o2.getCounter() == o3.getCounter() >= counter):
        o1._printStatus()
        o2._printStatus()
        o3._printStatus()
        print('Logs same:', o1._SyncObj__raftLog == o2._SyncObj__raftLog == o3._SyncObj__raftLog)
        print(time.time(), 'counters:', o1.getCounter(), o2.getCounter(), o3.getCounter(), counter)
        raise AssertionError('Values not equal')

    removeFiles(journalFiles)
    removeFiles([e + '.meta' for e in journalFiles])


# Ensure that raftLog after serialization is the same as in serialized data
def test_logCompactionRegressionTest1():
    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]])
    o2 = TestObj(a[1], [a[0]])
    objs = [o1, o2]

    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    o1._forceLogCompaction()
    doTicks(objs, 0.5)
    assert o1._SyncObj__forceLogCompaction == False
    logAfterCompaction = o1._SyncObj__raftLog
    o1._SyncObj__loadDumpFile(True)
    logAfterDeserialize = o1._SyncObj__raftLog
    assert logAfterCompaction == logAfterDeserialize
    o1._destroy()
    o2._destroy()


def test_logCompactionRegressionTest2():
    dumpFiles = [getNextDumpFile(), getNextDumpFile(), getNextDumpFile()]
    removeFiles(dumpFiles)

    random.seed(12)

    a = [getNextAddr(), getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1], a[2]], dumpFile=dumpFiles[0])
    o2 = TestObj(a[1], [a[2], a[0]], dumpFile=dumpFiles[1])
    o3 = TestObj(a[2], [a[0], a[1]], dumpFile=dumpFiles[2])
    objs = [o1, o2]

    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

    objs = [o1, o2, o3]
    o1.addValue(2)
    o1.addValue(3)
    doTicks(objs, 10, stopFunc=lambda: o3.getCounter() == 5)

    o3._forceLogCompaction()
    doTicks(objs, 0.5)

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader() == o3._getLeader()

    o3._destroy()

    objs = [o1, o2]

    o1.addValue(2)
    o1.addValue(3)

    doTicks(objs, 0.5)
    o1._forceLogCompaction()
    o2._forceLogCompaction()
    doTicks(objs, 0.5)

    o3 = TestObj(a[2], [a[0], a[1]], dumpFile=dumpFiles[2])
    objs = [o1, o2, o3]
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady() and o3._isReady())

    assert o1._isReady()
    assert o2._isReady()
    assert o3._isReady()

    o1._destroy()
    o2._destroy()
    o3._destroy()

    removeFiles(dumpFiles)


def __checkParnerNodeExists(obj, nodeAddr, shouldExist=True):
    nodeAddrSet = {node.address for node in obj._SyncObj__otherNodes}
    return (
                   nodeAddr in nodeAddrSet) == shouldExist  # either nodeAddr is in nodeAddrSet and shouldExist is True, or nodeAddr isn't in the set and shouldExist is False


def test_doChangeClusterUT1():
    dumpFiles = [getNextDumpFile()]
    removeFiles(dumpFiles)

    baseAddr = getNextAddr()
    oterAddr = getNextAddr()

    o1 = TestObj(baseAddr, ['localhost:1235', oterAddr], dumpFile=dumpFiles[0], dynamicMembershipChange=True)
    __checkParnerNodeExists(o1, 'localhost:1238', False)
    __checkParnerNodeExists(o1, 'localhost:1239', False)
    __checkParnerNodeExists(o1, 'localhost:1235', True)

    noop = _bchr(_COMMAND_TYPE.NO_OP)
    member = _bchr(_COMMAND_TYPE.MEMBERSHIP)

    # Check regular configuration change - adding
    o1._SyncObj__onMessageReceived(TCPNode('localhost:12345'), {
        'type': 'append_entries',
        'term': 1,
        'prevLogIdx': 1,
        'prevLogTerm': 0,
        'commit_index': 2,
        'entries': [(noop, 2, 1), (noop, 3, 1), (member + pickle.dumps(['add', 'localhost:1238']), 4, 1)]
    })
    __checkParnerNodeExists(o1, 'localhost:1238', True)
    __checkParnerNodeExists(o1, 'localhost:1239', False)

    # Check rollback adding
    o1._SyncObj__onMessageReceived(TCPNode('localhost:1236'), {
        'type': 'append_entries',
        'term': 2,
        'prevLogIdx': 2,
        'prevLogTerm': 1,
        'commit_index': 3,
        'entries': [(noop, 3, 2), (member + pickle.dumps(['add', 'localhost:1239']), 4, 2)]
    })
    __checkParnerNodeExists(o1, 'localhost:1238', False)
    __checkParnerNodeExists(o1, 'localhost:1239', True)
    __checkParnerNodeExists(o1, oterAddr, True)

    # Check regular configuration change - removing
    o1._SyncObj__onMessageReceived(TCPNode('localhost:1236'), {
        'type': 'append_entries',
        'term': 2,
        'prevLogIdx': 4,
        'prevLogTerm': 2,
        'commit_index': 4,
        'entries': [(member + pickle.dumps(['rem', 'localhost:1235']), 5, 2)]
    })

    __checkParnerNodeExists(o1, 'localhost:1238', False)
    __checkParnerNodeExists(o1, 'localhost:1239', True)
    __checkParnerNodeExists(o1, 'localhost:1235', False)

    # Check log compaction
    o1._forceLogCompaction()
    doTicks([o1], 0.5)
    o1._destroy()

    o2 = TestObj(oterAddr, [baseAddr, 'localhost:1236'], dumpFile='dump1.bin', dynamicMembershipChange=True)
    doTicks([o2], 0.5)

    __checkParnerNodeExists(o2, oterAddr, False)
    __checkParnerNodeExists(o2, baseAddr, True)
    __checkParnerNodeExists(o2, 'localhost:1238', False)
    __checkParnerNodeExists(o2, 'localhost:1239', True)
    __checkParnerNodeExists(o2, 'localhost:1235', False)
    o2._destroy()

    removeFiles(dumpFiles)


def test_doChangeClusterUT2():
    a = [getNextAddr(), getNextAddr(), getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1], a[2]], dynamicMembershipChange=True)
    o2 = TestObj(a[1], [a[2], a[0]], dynamicMembershipChange=True)
    o3 = TestObj(a[2], [a[0], a[1]], dynamicMembershipChange=True)

    doTicks([o1, o2, o3], 10, stopFunc=lambda: o1._isReady() and o2._isReady() and o3._isReady())

    assert o1._isReady() == o2._isReady() == o3._isReady() == True
    o3.addValue(50)
    o2.addNodeToCluster(a[3])

    success = False
    for i in xrange(10):
        doTicks([o1, o2, o3], 0.5)
        res = True
        res &= __checkParnerNodeExists(o1, a[3], True)
        res &= __checkParnerNodeExists(o2, a[3], True)
        res &= __checkParnerNodeExists(o3, a[3], True)
        if res:
            success = True
            break
        o2.addNodeToCluster(a[3])

    assert success

    o4 = TestObj(a[3], [a[0], a[1], a[2]], dynamicMembershipChange=True)
    doTicks([o1, o2, o3, o4], 10, stopFunc=lambda: o4._isReady())
    o1.addValue(450)
    doTicks([o1, o2, o3, o4], 10, stopFunc=lambda: o4.getCounter() == 500)
    assert o4.getCounter() == 500

    o1._destroy()
    o2._destroy()
    o3._destroy()
    o4._destroy()


def test_journalTest1():
    dumpFiles = [getNextDumpFile(), getNextDumpFile()]
    journalFiles = [getNextJournalFile(), getNextJournalFile()]
    removeFiles(dumpFiles)
    removeFiles(journalFiles)
    removeFiles([e + '.meta' for e in journalFiles])

    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[0], journalFile=journalFiles[0])
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[1], journalFile=journalFiles[1])
    objs = [o1, o2]
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    o1.addValue(150)
    o2.addValue(200)

    doTicks(objs, 10, stopFunc=lambda: o1.getCounter() == 350 and o2.getCounter() == 350)

    assert o1.getCounter() == 350
    assert o2.getCounter() == 350

    o1._destroy()
    o2._destroy()

    a = [getNextAddr(), getNextAddr()]
    o1 = TestObj(a[0], [a[1]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[0], journalFile=journalFiles[0])
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[1], journalFile=journalFiles[1])
    objs = [o1, o2]
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady() and \
                                       o1.getCounter() == 350 and o2.getCounter() == 350)
    assert o1._isReady()
    assert o2._isReady()

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    assert o1.getCounter() == 350
    assert o2.getCounter() == 350

    o1.addValue(100)
    o2.addValue(150)

    doTicks(objs, 10, stopFunc=lambda: o1.getCounter() == 600 and o2.getCounter() == 600)

    assert o1.getCounter() == 600
    assert o2.getCounter() == 600

    o1._forceLogCompaction()
    o2._forceLogCompaction()

    doTicks(objs, 0.5)

    o1.addValue(150)
    o2.addValue(150)

    doTicks(objs, 10, stopFunc=lambda: o1.getCounter() == 900 and o2.getCounter() == 900)

    assert o1.getCounter() == 900
    assert o2.getCounter() == 900

    o1._destroy()
    o2._destroy()

    a = [getNextAddr(), getNextAddr()]
    o1 = TestObj(a[0], [a[1]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[0], journalFile=journalFiles[0])
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[1], journalFile=journalFiles[1])
    objs = [o1, o2]
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady() and \
                                       o1.getCounter() == 900 and o2.getCounter() == 900)
    assert o1._isReady()
    assert o2._isReady()

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    assert o1.getCounter() == 900
    assert o2.getCounter() == 900

    o1._destroy()
    o2._destroy()

    removeFiles(dumpFiles)
    removeFiles(journalFiles)
    removeFiles([e + '.meta' for e in journalFiles])


def test_journalTest2():
    journalFiles = [getNextJournalFile()]
    removeFiles(journalFiles)
    removeFiles([e + '.meta' for e in journalFiles])

    removeFiles(journalFiles)
    journal = createJournal(journalFiles[0])
    journal.add(b'cmd1', 1, 0)
    journal.add(b'cmd2', 2, 0)
    journal.add(b'cmd3', 3, 0)
    journal._destroy()

    journal = createJournal(journalFiles[0])
    assert len(journal) == 3
    assert journal[0] == (b'cmd1', 1, 0)
    assert journal[-1] == (b'cmd3', 3, 0)
    journal.deleteEntriesFrom(2)
    journal._destroy()

    journal = createJournal(journalFiles[0])
    assert len(journal) == 2
    assert journal[0] == (b'cmd1', 1, 0)
    assert journal[-1] == (b'cmd2', 2, 0)
    journal.deleteEntriesTo(1)
    journal._destroy()

    journal = createJournal(journalFiles[0])
    assert len(journal) == 1
    assert journal[0] == (b'cmd2', 2, 0)
    journal._destroy()
    removeFiles(journalFiles)
    removeFiles([e + '.meta' for e in journalFiles])


def test_applyJournalAfterRestart():
    dumpFiles = [getNextDumpFile(), getNextDumpFile()]
    journalFiles = [getNextJournalFile(), getNextJournalFile()]
    removeFiles(dumpFiles)
    removeFiles(journalFiles)
    removeFiles([e + '.meta' for e in journalFiles])

    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[0], journalFile=journalFiles[0])
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[1], journalFile=journalFiles[1])
    objs = [o1, o2]
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    o1.addValue(150)
    o2.addValue(200)

    doTicks(objs, 10, stopFunc=lambda: o1.getCounter() == 350 and o2.getCounter() == 350)

    assert o1.getCounter() == 350
    assert o2.getCounter() == 350
    doTicks(objs, 2)

    o1._destroy()
    o2._destroy()

    removeFiles(dumpFiles)

    o1 = TestObj(a[0], [a[1]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[0], journalFile=journalFiles[0])
    objs = [o1]
    doTicks(objs, 10, o1.getCounter() == 350)

    assert o1.getCounter() == 350

    removeFiles(dumpFiles)
    removeFiles(journalFiles)
    removeFiles([e + '.meta' for e in journalFiles])


def test_autoTick1():
    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], TEST_TYPE.AUTO_TICK_1)
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.AUTO_TICK_1)

    assert not o1._isReady()
    assert not o2._isReady()

    time.sleep(4.5)
    assert o1._isReady()
    assert o2._isReady()

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()
    assert o1._isReady()
    assert o2._isReady()

    o1.addValue(150)
    o2.addValue(200)

    time.sleep(1.5)

    assert o1._isReady()
    assert o2._isReady()

    assert o1.getCounter() == 350
    assert o2.getCounter() == 350

    assert o2.addValueSync(10) == 360
    assert o1.addValueSync(20) == 380

    o1._destroy()
    o2._destroy()
    time.sleep(0.5)


def test_largeCommands():
    dumpFiles = [getNextDumpFile(), getNextDumpFile()]
    removeFiles(dumpFiles)

    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], TEST_TYPE.LARGE_COMMAND, dumpFile=dumpFiles[0], leaderFallbackTimeout=60.0)
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.LARGE_COMMAND, dumpFile=dumpFiles[1], leaderFallbackTimeout=60.0)
    objs = [o1, o2]
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    # Generate ~20Mb data.
    testRandStr = getRandStr()
    bigStr = ''
    for i in xrange(0, 200):
        bigStr += getRandStr()
    o1.addKeyValue('big', bigStr)
    o1.addKeyValue('test', testRandStr)

    # Wait for replication.
    doTicks(objs, 60, stopFunc=lambda: o1.getValue('test') == testRandStr and \
                                       o2.getValue('test') == testRandStr and \
                                       o1.getValue('big') == bigStr and \
                                       o2.getValue('big') == bigStr)

    assert o1.getValue('test') == testRandStr
    assert o2.getValue('big') == bigStr

    o1._forceLogCompaction()
    o2._forceLogCompaction()

    # Wait for disk dump
    doTicks(objs, 8.0)

    o1._destroy()
    o2._destroy()

    a = [getNextAddr(), getNextAddr()]
    o1 = TestObj(a[0], [a[1]], TEST_TYPE.LARGE_COMMAND, dumpFile=dumpFiles[0], leaderFallbackTimeout=60.0)
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.LARGE_COMMAND, dumpFile=dumpFiles[1], leaderFallbackTimeout=60.0)
    objs = [o1, o2]
    # Wait for disk load, election and replication

    doTicks(objs, 60, stopFunc=lambda: o1.getValue('test') == testRandStr and \
                                       o2.getValue('test') == testRandStr and \
                                       o1.getValue('big') == bigStr and \
                                       o2.getValue('big') == bigStr and \
                                       o1._isReady() and o2._isReady())

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    assert o1.getValue('test') == testRandStr
    assert o2.getValue('big') == bigStr
    assert o1.getValue('test') == testRandStr
    assert o2.getValue('big') == bigStr

    o1._destroy()
    o2._destroy()

    removeFiles(dumpFiles)

@pytest.mark.skipif(platform.python_implementation() != 'CPython', reason="does not have crypto on pypy")
def test_readOnlyNodes():
    random.seed(12)

    a = [getNextAddr(), getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1], a[2]], password='123')
    o2 = TestObj(a[1], [a[2], a[0]], password='123')
    o3 = TestObj(a[2], [a[0], a[1]], password='123')
    objs = [o1, o2, o3]

    b1 = TestObj(None, [a[0], a[1], a[2]], password='123')
    b2 = TestObj(None, [a[0], a[1], a[2]], password='123')

    roObjs = [b1, b2]

    doTicks(objs, 10.0, stopFunc=lambda: o1._isReady() and o2._isReady() and o3._isReady())

    assert o1._isReady()
    assert o2._isReady()
    assert o3._isReady()

    o1.addValue(150)
    o2.addValue(200)

    doTicks(objs, 10.0, stopFunc=lambda: o3.getCounter() == 350)

    doTicks(objs + roObjs, 4.0, stopFunc=lambda: b1.getCounter() == 350 and b2.getCounter() == 350)

    assert b1.getCounter() == b2.getCounter() == 350
    assert o1._getLeader() == b1._getLeader() == o2._getLeader() == b2._getLeader()
    assert b1._getLeader().address in a

    prevLeader = o1._getLeader()

    newObjs = [o for o in objs if o._SyncObj__selfNode != prevLeader]

    assert len(newObjs) == 2

    doTicks(newObjs + roObjs, 10.0, stopFunc=lambda: newObjs[0]._getLeader() != prevLeader and \
                                                     newObjs[0]._getLeader() is not None and \
                                                     newObjs[0]._getLeader().address in a and \
                                                     newObjs[0]._getLeader() == newObjs[1]._getLeader())

    assert newObjs[0]._getLeader() != prevLeader
    assert newObjs[0]._getLeader().address in a
    assert newObjs[0]._getLeader() == newObjs[1]._getLeader()

    newObjs[1].addValue(50)

    doTicks(newObjs + roObjs, 10.0, stopFunc=lambda: newObjs[0].getCounter() == 400 and b1.getCounter() == 400)

    o1._printStatus()
    o2._printStatus()
    o3._printStatus()

    b1._printStatus()

    assert newObjs[0].getCounter() == 400
    assert b1.getCounter() == 400

    doTicks(objs + roObjs, 10.0,
            stopFunc=lambda: sum([int(o.getCounter() == 400) for o in objs + roObjs]) == len(objs + roObjs))

    for o in objs + roObjs:
        assert o.getCounter() == 400

    currRes = {}

    def onAdd(res, err):
        currRes[0] = err

    b1.addValue(50, callback=onAdd)

    doTicks(objs + roObjs, 5.0, stopFunc=lambda: o1.getCounter() == 450 and \
                                                 b1.getCounter() == 450 and \
                                                 b2.getCounter() == 450 and
                                                 currRes.get(0) == FAIL_REASON.SUCCESS)
    assert o1.getCounter() == 450
    assert b1.getCounter() == 450
    assert b2.getCounter() == 450
    assert currRes.get(0) == FAIL_REASON.SUCCESS


    # check that all objects have 2 readonly nodes
    assert all(map(lambda o: o.getStatus()['readonly_nodes_count'] == 2, objs))

    # disconnect readonly node
    b1._destroy()
    doTicks(objs, 2.0)

    assert all(map(lambda o: o.getStatus()['readonly_nodes_count'] == 1, objs))



    o1._destroy()
    o2._destroy()
    o3._destroy()

    b1._destroy()
    b2._destroy()


@pytest.mark.skipif(platform.python_implementation() != 'CPython', reason="does not have crypto on pypy")
def test_syncobjAdminStatus():
    assert HAS_CRYPTO

    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], password='123')
    o2 = TestObj(a[1], [a[0]], password='123')

    assert not o1._isReady()
    assert not o2._isReady()

    doTicks([o1, o2], 10.0, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._isReady()
    assert o2._isReady()

    status1 = o1.getStatus()
    status2 = o2.getStatus()

    assert 'version' in status1
    assert 'log_len' in status2

    trueRes = {
        o1: '\n'.join('%s: %s' % (k, v) for k, v in sorted(status1.items())),
        o2: '\n'.join('%s: %s' % (k, v) for k, v in sorted(status2.items())),
    }

    currRes = {
    }
    args = {
        o1: ['-conn', a[0], '-pass', '123', '-status'],
        o2: ['-conn', a[1], '-pass', '123', '-status'],
    }
    doSyncObjAdminTicks([o1, o2], args, 10.0, currRes,
                        stopFunc=lambda: currRes.get(o1) is not None and currRes.get(o2) is not None)

    assert len(currRes[o1]) == len(trueRes[o1])
    assert len(currRes[o2]) == len(trueRes[o2])

    o1._destroy()
    o2._destroy()


def test_syncobjAdminAddRemove():
    random.seed(42)

    a = [getNextAddr(), getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], dynamicMembershipChange=True)
    o2 = TestObj(a[1], [a[0]], dynamicMembershipChange=True)

    assert not o1._isReady()
    assert not o2._isReady()

    doTicks([o1, o2], 10.0, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._isReady()
    assert o2._isReady()

    trueRes = 'SUCCESS ADD ' + a[2]

    currRes = {}

    args = {
        o1: ['-conn', a[0], '-add', a[2]],
    }

    doSyncObjAdminTicks([o1, o2], args, 10.0, currRes, stopFunc=lambda: currRes.get(o1) is not None)

    assert currRes[o1] == trueRes

    o3 = TestObj(a[2], [a[1], a[0]], dynamicMembershipChange=True)

    doTicks([o1, o2, o3], 10.0, stopFunc=lambda: o1._isReady() and o2._isReady() and o3._isReady())

    assert o1._isReady()
    assert o2._isReady()
    assert o3._isReady()

    trueRes = 'SUCCESS REMOVE ' + a[2]
    args[o1] = None
    args[o2] = ['-conn', a[1], '-remove', a[2]]

    doSyncObjAdminTicks([o1, o2, o3], args, 10.0, currRes, stopFunc=lambda: currRes.get(o2) is not None)

    assert currRes[o2] == trueRes

    o3._destroy()

    doTicks([o1, o2], 10.0, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._isReady()
    assert o2._isReady()

    o1._destroy()
    o2._destroy()


def test_journalWithAddNodes():
    dumpFiles = [getNextDumpFile(), getNextDumpFile(), getNextDumpFile()]
    journalFiles = [getNextJournalFile(), getNextJournalFile(), getNextJournalFile()]
    removeFiles(dumpFiles)
    removeFiles(journalFiles)
    removeFiles([e + '.meta' for e in journalFiles])

    random.seed(42)

    a = [getNextAddr(), getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[0], journalFile=journalFiles[0], dynamicMembershipChange=True)
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[1], journalFile=journalFiles[1], dynamicMembershipChange=True)
    objs = [o1, o2]
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()

    o1.addValue(150)
    o2.addValue(200)

    doTicks(objs, 10, stopFunc=lambda: o1.getCounter() == 350 and o2.getCounter() == 350)

    assert o1.getCounter() == 350
    assert o2.getCounter() == 350
    doTicks(objs, 2)


    trueRes = 'SUCCESS ADD ' + a[2]
    currRes = {}
    args = {
        o1: ['-conn', a[0], '-add', a[2]],
    }
    doSyncObjAdminTicks([o1, o2], args, 10.0, currRes, stopFunc=lambda: currRes.get(o1) is not None)

    assert currRes[o1] == trueRes

    o3 = TestObj(a[2], [a[1], a[0]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[2], journalFile=journalFiles[2], dynamicMembershipChange=True)

    doTicks([o1, o2, o3], 10.0, stopFunc=lambda: o1._isReady() and o2._isReady() and o3._isReady())

    assert o1._isReady()
    assert o2._isReady()
    assert o3._isReady()

    assert o3.getCounter() == 350

    doTicks(objs, 2)


    o1._destroy()
    o2._destroy()
    o3._destroy()

    removeFiles(dumpFiles)

    o1 = TestObj(a[0], [a[1]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[0], journalFile=journalFiles[0], dynamicMembershipChange=True)
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[1], journalFile=journalFiles[1], dynamicMembershipChange=True)
    o3 = TestObj(a[2], [a[1], a[0]], TEST_TYPE.JOURNAL_1, dumpFile=dumpFiles[2], journalFile=journalFiles[2], dynamicMembershipChange=True)

    objs = [o1, o2, o3]
    doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o1.getCounter() == 350 and o3._isReady() and o3.getCounter() == 350)

    assert o1._isReady()
    assert o3._isReady()

    assert o1.getCounter() == 350
    assert o3.getCounter() == 350

    o2.addValue(200)

    doTicks(objs, 10, stopFunc=lambda: o1.getCounter() == 550 and o3.getCounter() == 550)

    assert o1.getCounter() == 550
    assert o3.getCounter() == 550

    o1._destroy()
    o2._destroy()
    o3._destroy()

    removeFiles(dumpFiles)
    removeFiles(journalFiles)
    removeFiles([e + '.meta' for e in journalFiles])


def test_syncobjAdminSetVersion():
    random.seed(42)

    a = [getNextAddr(), getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], dynamicMembershipChange=True)
    o2 = TestObj(a[1], [a[0]], dynamicMembershipChange=True)

    assert not o1._isReady()
    assert not o2._isReady()

    doTicks([o1, o2], 10.0, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._isReady()
    assert o2._isReady()

    assert o1.getCodeVersion() == 0
    assert o2.getCodeVersion() == 0

    o2.testMethod()

    doTicks([o1, o2], 10.0, stopFunc=lambda: o1.getValue('testKey') == 'valueVer1' and \
                                             o2.getValue('testKey') == 'valueVer1')

    assert o1.getValue('testKey') == 'valueVer1'
    assert o2.getValue('testKey') == 'valueVer1'

    trueRes = 'SUCCESS SET_VERSION 1'

    currRes = {}

    args = {
        o1: ['-conn', a[0], '-set_version', '1'],
    }

    doSyncObjAdminTicks([o1, o2], args, 10.0, currRes, stopFunc=lambda: currRes.get(o1) is not None)

    assert currRes[o1] == trueRes

    doTicks([o1, o2], 10.0, stopFunc=lambda: o1.getCodeVersion() == 1 and o2.getCodeVersion() == 1)

    assert o1.getCodeVersion() == 1
    assert o2.getCodeVersion() == 1

    o2.testMethod()

    doTicks([o1, o2], 10.0, stopFunc=lambda: o1.getValue('testKey') == 'valueVer2' and \
                                             o2.getValue('testKey') == 'valueVer2')

    assert o1.getValue('testKey') == 'valueVer2'
    assert o2.getValue('testKey') == 'valueVer2'

    o1._destroy()
    o2._destroy()


@pytest.mark.skipif(os.name == 'nt', reason='temporary disabled for windows')
def test_syncobjWaitBinded():
    random.seed(42)

    a = [getNextAddr(), getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], testType=TEST_TYPE.WAIT_BIND)
    o2 = TestObj(a[1], [a[0]], testType=TEST_TYPE.WAIT_BIND)

    o1.waitBinded()
    o2.waitBinded()

    o3 = TestObj(a[1], [a[0]], testType=TEST_TYPE.WAIT_BIND)
    with pytest.raises(SyncObjException):
        o3.waitBinded()

    o1.destroy()
    o2.destroy()
    o3.destroy()


@pytest.mark.skipif(os.name == 'nt', reason='temporary disabled for windows')
def test_unpickle():
    data = {'foo': 'bar', 'command': b'\xfa', 'entries': [b'\xfb', b'\xfc']}
    python2_cpickle = b'\x80\x02}q\x01(U\x03fooq\x02U\x03barq\x03U\x07commandq\x04U\x01\xfaU\x07entriesq\x05]q\x06(U\x01\xfbU\x01\xfceu.'
    python2_pickle = b'\x80\x02}q\x00(U\x03fooq\x01U\x03barq\x02U\x07commandq\x03U\x01\xfaq\x04U\x07entriesq\x05]q\x06(U\x01\xfbq\x07U\x01\xfcq\x08eu.'
    python3_pickle = b'\x80\x02}q\x00(X\x03\x00\x00\x00fooq\x01X\x03\x00\x00\x00barq\x02X\x07\x00\x00\x00commandq\x03c_codecs\nencode\nq\x04X\x02\x00\x00\x00\xc3\xbaq\x05X\x06\x00\x00\x00latin1q\x06\x86q\x07Rq\x08X\x07\x00\x00\x00entriesq\t]q\n(h\x04X\x02\x00\x00\x00\xc3\xbbq\x0bh\x06\x86q\x0cRq\rh\x04X\x02\x00\x00\x00\xc3\xbcq\x0eh\x06\x86q\x0fRq\x10eu.'

    python2_cpickle_data = pickle.loads(python2_cpickle)
    assert data == python2_cpickle_data, 'Failed to unpickle data pickled by python2 cPickle'

    python2_pickle_data = pickle.loads(python2_pickle)
    assert data == python2_pickle_data, 'Failed to unpickle data pickled by python2 pickle'

    python3_pickle_data = pickle.loads(python3_pickle)
    assert data == python3_pickle_data, 'Failed to unpickle data pickled by python3 pickle'


class TestConsumer1(SyncObjConsumer):
    def __init__(self):
        super(TestConsumer1, self).__init__()
        self.__counter = 0

    @replicated
    def add(self, value):
        self.__counter += value

    @replicated
    def set(self, value):
        self.__counter = value

    def get(self):
        return self.__counter


class TestConsumer2(SyncObjConsumer):
    def __init__(self):
        super(TestConsumer2, self).__init__()
        self.__values = {}

    @replicated
    def set(self, key, value):
        self.__values[key] = value

    def get(self, key):
        return self.__values.get(key)


def test_consumers():
    random.seed(42)

    a = [getNextAddr(), getNextAddr(), getNextAddr()]

    c11 = TestConsumer1()
    c12 = TestConsumer1()
    c13 = TestConsumer2()

    c21 = TestConsumer1()
    c22 = TestConsumer1()
    c23 = TestConsumer2()

    c31 = TestConsumer1()
    c32 = TestConsumer1()
    c33 = TestConsumer2()

    o1 = TestObj(a[0], [a[1], a[2]], consumers=[c11, c12, c13])
    o2 = TestObj(a[1], [a[0], a[2]], consumers=[c21, c22, c23])
    o3 = TestObj(a[2], [a[0], a[1]], consumers=[c31, c32, c33])
    objs = [o1, o2]

    assert not o1._isReady()
    assert not o2._isReady()

    doTicks(objs, 10.0, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()
    assert o1._isReady()
    assert o2._isReady()

    c11.set(42)
    c11.add(10)
    c12.add(15)
    c13.set('testKey', 'testValue')
    doTicks(objs, 10.0, stopFunc=lambda: c21.get() == 52 and c22.get() == 15 and c23.get('testKey') == 'testValue')

    assert c21.get() == 52
    assert c22.get() == 15
    assert c23.get('testKey') == 'testValue'

    o1.forceLogCompaction()
    o2.forceLogCompaction()
    doTicks(objs, 0.5)
    objs = [o1, o2, o3]

    doTicks(objs, 10.0, stopFunc=lambda: c31.get() == 52 and c32.get() == 15 and c33.get('testKey') == 'testValue')

    assert c31.get() == 52
    assert c32.get() == 15
    assert c33.get('testKey') == 'testValue'

    o1.destroy()
    o2.destroy()
    o3.destroy()


def test_batteriesCommon():
    d1 = ReplDict()
    l1 = ReplLockManager(autoUnlockTime=30.0)

    d2 = ReplDict()
    l2 = ReplLockManager(autoUnlockTime=30.0)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], TEST_TYPE.AUTO_TICK_1, consumers=[d1, l1])
    o2 = TestObj(a[1], [a[0]], TEST_TYPE.AUTO_TICK_1, consumers=[d2, l2])

    doAutoTicks(10.0, stopFunc=lambda: o1.isReady() and o2.isReady())

    assert o1.isReady() and o2.isReady()

    d1.set('testKey', 'testValue', sync=True)
    doAutoTicks(3.0, stopFunc=lambda: d2.get('testKey') == 'testValue')

    assert d2['testKey'] == 'testValue'

    d2.pop('testKey', sync=True)
    doAutoTicks(3.0, stopFunc=lambda: d1.get('testKey') == None)

    assert d1.get('testKey') == None

    assert l1.tryAcquire('test.lock1', sync=True) == True
    assert l2.tryAcquire('test.lock1', sync=True) == False
    assert l2.isAcquired('test.lock1') == False

    l1id = l1._ReplLockManager__selfID
    l1._ReplLockManager__lockImpl.prolongate(l1id, 0, _doApply=True)

    l1.release('test.lock1', sync=True)
    assert l2.tryAcquire('test.lock1', sync=True) == True

    assert d1.setdefault('keyA', 'valueA', sync=True) == 'valueA'
    assert d2.setdefault('keyA', 'valueB', sync=True) == 'valueA'
    d2.pop('keyA', sync=True)
    assert d2.setdefault('keyA', 'valueB', sync=True) == 'valueB'

    o1.destroy()
    o2.destroy()

    l1.destroy()
    l2.destroy()


def test_ReplCounter():
    c = ReplCounter()
    c.set(42, _doApply=True)
    assert c.get() == 42
    c.add(10, _doApply=True)
    assert c.get() == 52
    c.sub(20, _doApply=True)
    assert c.get() == 32
    c.inc(_doApply=True)
    assert c.get() == 33


def test_ReplList():
    l = ReplList()
    l.reset([1, 2, 3], _doApply=True)
    assert l.rawData() == [1, 2, 3]
    l.set(1, 10, _doApply=True)
    assert l.rawData() == [1, 10, 3]
    l.append(42, _doApply=True)
    assert l.rawData() == [1, 10, 3, 42]
    l.extend([5, 6], _doApply=True)
    assert l.rawData() == [1, 10, 3, 42, 5, 6]
    l.insert(2, 66, _doApply=True)
    assert l.rawData() == [1, 10, 66, 3, 42, 5, 6]
    l.remove(66, _doApply=True)
    assert l.rawData() == [1, 10, 3, 42, 5, 6]
    l.pop(1, _doApply=True)
    assert l.rawData() == [1, 3, 42, 5, 6]
    l.sort(reverse=True, _doApply=True)
    assert l.rawData() == [42, 6, 5, 3, 1]
    assert l.index(6) == 1
    assert l.count(42) == 1
    assert l.get(2) == 5
    assert l[4] == 1
    assert len(l) == 5
    l.__setitem__(0, 43, _doApply=True)
    assert l[0] == 43


def test_ReplDict():
    d = ReplDict()

    d.reset({
        1: 1,
        2: 22,
    }, _doApply=True)
    assert d.rawData() == {
        1: 1,
        2: 22,
    }

    d.__setitem__(1, 10, _doApply=True)
    assert d.rawData() == {
        1: 10,
        2: 22,
    }

    d.set(1, 20, _doApply=True)
    assert d.rawData() == {
        1: 20,
        2: 22,
    }

    assert d.setdefault(1, 50, _doApply=True) == 20
    assert d.setdefault(3, 50, _doApply=True) == 50

    d.update({
        5: 5,
        6: 7,
    }, _doApply=True)

    assert d.rawData() == {
        1: 20,
        2: 22,
        3: 50,
        5: 5,
        6: 7,
    }

    assert d.pop(3, _doApply=True) == 50
    assert d.pop(6, _doApply=True) == 7
    assert d.pop(6, _doApply=True) == None
    assert d.pop(6, 0, _doApply=True) == 0

    assert d.rawData() == {
        1: 20,
        2: 22,
        5: 5,
    }

    assert d[1] == 20
    assert d.get(2) == 22
    assert d.get(22) == None
    assert d.get(22, 10) == 10
    assert len(d) == 3
    assert 2 in d
    assert 22 not in d
    assert sorted(d.keys()) == [1, 2, 5]
    assert sorted(d.values()) == [5, 20, 22]
    assert d.items() == d.rawData().items()

    d.clear(_doApply=True)
    assert len(d) == 0


def test_ReplSet():
    s = ReplSet()
    s.reset({1, 4}, _doApply=True)
    assert s.rawData() == {1, 4}

    s.add(10, _doApply=True)
    assert s.rawData() == {1, 4, 10}

    s.remove(1, _doApply=True)
    s.discard(10, _doApply=True)
    assert s.rawData() == {4}

    assert s.pop(_doApply=True) == 4

    s.add(48, _doApply=True)
    s.update({9, 2, 3}, _doApply=True)

    assert s.rawData() == {9, 2, 3, 48}

    assert len(s) == 4
    assert 9 in s
    assert 42 not in s

    s.clear(_doApply=True)
    assert len(s) == 0
    assert 9 not in s


def test_ReplQueue():
    q = ReplQueue()
    q.put(42, _doApply=True)
    q.put(33, _doApply=True)
    q.put(14, _doApply=True)

    assert q.get(_doApply=True) == 42

    assert q.qsize() == 2
    assert len(q) == 2

    assert q.empty() == False

    assert q.get(_doApply=True) == 33
    assert q.get(-1, _doApply=True) == 14
    assert q.get(_doApply=True) == None
    assert q.get(-1, _doApply=True) == -1
    assert q.empty()

    q = ReplQueue(3)
    q.put(42, _doApply=True)
    q.put(33, _doApply=True)
    assert q.full() == False
    assert q.put(14, _doApply=True) == True
    assert q.full() == True
    assert q.put(19, _doApply=True) == False
    assert q.get(_doApply=True) == 42


def test_ReplPriorityQueue():
    q = ReplPriorityQueue()
    q.put(42, _doApply=True)
    q.put(14, _doApply=True)
    q.put(33, _doApply=True)

    assert q.get(_doApply=True) == 14

    assert q.qsize() == 2
    assert len(q) == 2

    assert q.empty() == False

    assert q.get(_doApply=True) == 33
    assert q.get(-1, _doApply=True) == 42
    assert q.get(_doApply=True) == None
    assert q.get(-1, _doApply=True) == -1
    assert q.empty()

    q = ReplPriorityQueue(3)
    q.put(42, _doApply=True)
    q.put(33, _doApply=True)
    assert q.full() == False
    assert q.put(14, _doApply=True) == True
    assert q.full() == True
    assert q.put(19, _doApply=True) == False
    assert q.get(_doApply=True) == 14


# https://github.com/travis-ci/travis-ci/issues/8695
@pytest.mark.skipif(os.name == 'nt' or os.environ.get('TRAVIS') == 'true', reason='temporary disabled for windows')
def test_ipv6():
    random.seed(42)

    a = [getNextAddr(ipv6=True), getNextAddr(ipv6=True)]

    o1 = TestObj(a[0], [a[1]])
    o2 = TestObj(a[1], [a[0]])
    objs = [o1, o2]

    assert not o1._isReady()
    assert not o2._isReady()

    doTicks(objs, 10.0, stopFunc=lambda: o1._isReady() and o2._isReady())

    assert o1._isReady()
    assert o2._isReady()

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()
    assert o1._isReady()
    assert o2._isReady()

    o1.addValue(150)
    o2.addValue(200)

    doTicks(objs, 10.0, stopFunc=lambda: o1.getCounter() == 350 and o2.getCounter() == 350)

    assert o1._isReady()
    assert o2._isReady()

    assert o1.getCounter() == 350
    assert o2.getCounter() == 350

    o1._destroy()
    o2._destroy()


def test_localhost():
    random.seed(42)

    a = [getNextAddr(isLocalhost=True), getNextAddr(isLocalhost=True)]

    o1 = TestObj(a[0], [a[1]])
    o2 = TestObj(a[1], [a[0]])
    objs = [o1, o2]

    assert not o1._isReady()
    assert not o2._isReady()

    doTicks(objs, 3.0, stopFunc=lambda: o1._isReady() and o2._isReady())

    o1.waitBinded()
    o2.waitBinded()

    o1._printStatus()

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()
    assert o1._isReady()
    assert o2._isReady()

    o1.addValue(150)
    o2.addValue(200)

    doTicks(objs, 10.0, stopFunc=lambda: o1.getCounter() == 350 and o2.getCounter() == 350)

    assert o1._isReady()
    assert o2._isReady()

    assert o1.getCounter() == 350
    assert o2.getCounter() == 350

    o1._destroy()
    o2._destroy()


def test_leaderFallback():
    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]], leaderFallbackTimeout=30.0)
    o2 = TestObj(a[1], [a[0]], leaderFallbackTimeout=30.0)
    objs = [o1, o2]

    assert not o1._isReady()
    assert not o2._isReady()

    doTicks(objs, 5.0, stopFunc=lambda: o1._isReady() and o2._isReady())

    o1._SyncObj__conf.leaderFallbackTimeout = 3.0
    o2._SyncObj__conf.leaderFallbackTimeout = 3.0

    doTicks([o for o in objs if o._isLeader()], 2.0)

    assert o1._isLeader() or o2._isLeader()

    doTicks([o for o in objs if o._isLeader()], 2.0)

    assert not o1._isLeader() and not o2._isLeader()


class ZeroDeployConsumerAlpha(SyncObjConsumer):
    @replicated(ver=1)
    def someMethod(self):
        pass

    @replicated
    def methodTwo(self):
        pass


class ZeroDeployConsumerBravo(SyncObjConsumer):
    @replicated
    def alphaMethod(self):
        pass

    @replicated(ver=3)
    def methodTwo(self):
        pass


class ZeroDeployTestObj(SyncObj):
    def __init__(self, selfAddr, otherAddrs, consumers):
        cfg = SyncObjConf(autoTick=False)
        super(ZeroDeployTestObj, self).__init__(selfAddr, otherAddrs, cfg, consumers=consumers)

    @replicated
    def someMethod(self):
        pass

    @replicated
    def otherMethod(self):
        pass

    @replicated(ver=1)
    def thirdMethod(self):
        pass

    @replicated(ver=2)
    def lastMethod(self):
        pass

    @replicated(ver=3)
    def lastMethod(self):
        pass


def test_zeroDeployVersions():
    random.seed(42)

    a = [getNextAddr()]

    cAlpha = ZeroDeployConsumerAlpha()
    cBravo = ZeroDeployConsumerBravo()

    o1 = ZeroDeployTestObj(a[0], [], [cAlpha, cBravo])

    assert hasattr(o1, 'otherMethod_v0') == True
    assert hasattr(o1, 'lastMethod_v2') == True
    assert hasattr(o1, 'lastMethod_v3') == True
    assert hasattr(o1, 'lastMethod_v4') == False
    assert hasattr(cAlpha, 'methodTwo_v0') == True
    assert hasattr(cBravo, 'methodTwo_v3') == True

    assert o1._methodToID['lastMethod_v2'] > o1._methodToID['otherMethod_v0']
    assert o1._methodToID['lastMethod_v3'] > o1._methodToID['lastMethod_v2']
    assert o1._methodToID['lastMethod_v3'] > o1._methodToID['someMethod_v0']
    assert o1._methodToID['thirdMethod_v1'] > o1._methodToID['someMethod_v0']

    assert o1._methodToID['lastMethod_v2'] > o1._methodToID[(id(cAlpha), 'methodTwo_v0')]
    assert o1._methodToID[id(cBravo), 'methodTwo_v3'] > o1._methodToID['lastMethod_v2']

    assert 'someMethod' not in o1._methodToID
    assert 'thirdMethod' not in o1._methodToID
    assert 'lastMethod' not in o1._methodToID


def test_dnsResolverBug(monkeypatch):
    monkeypatch.setattr(dns_resolver, "monotonicTime", lambda: 0.0)
    resolver = dns_resolver.DnsCachingResolver(600, 30)
    ip = resolver.resolve('localhost')
    assert ip == '127.0.0.1'

class MockSocket(object):
    def __init__(self, socket, numSuccessSends):
        self.socket = socket
        self.numSuccessSends = numSuccessSends
    def send(self, data):
        self.numSuccessSends -= 1
        if self.numSuccessSends <= 0:
            return -100500
        return self.socket.send(data)
    def close(self):
        return self.socket.close()
    def getsockopt(self, *args, **kwargs):
        return self.socket.getsockopt(*args, **kwargs)
    def recv(self, *args, **kwargs):
        return self.socket.recv(*args, **kwargs)

def setMockSocket(o, numSuccess = 0):
    for readonlyNode in o._SyncObj__readonlyNodes:
        for node, conn in o._SyncObj__transport._connections.items():
            if node == readonlyNode:
                origSocket = conn._TcpConnection__socket
                conn._TcpConnection__socket = MockSocket(origSocket, numSuccess)
                #origSend = origSocket.send
                #origSocket.send = lambda x: mockSend(origSend, x)
                #print("Set mock send")

def test_readOnlyDrop():
    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1]])
    o2 = TestObj(a[1], [a[0]])
    o3 = TestObj(None, [a[0], a[1]])
    objs = [o1, o2, o3]

    assert not o1._isReady()
    assert not o2._isReady()
    assert not o3._isReady()

    doTicks(objs, 10.0, stopFunc=lambda: o1._isReady() and o2._isReady() and o3._isReady())

    o1.waitBinded()
    o2.waitBinded()

    o1._printStatus()

    assert o1._getLeader().address in a
    assert o1._getLeader() == o2._getLeader()
    assert o1._isReady()
    assert o2._isReady()
    assert o3._isReady()

    o1.addValue(150)
    o2.addValue(200)

    doTicks(objs, 10.0, stopFunc=lambda: o1.getCounter() == 350 and o2.getCounter() == 350 and o3.getCounter() == 350)

    assert o1._isReady()
    assert o2._isReady()

    assert o1.getCounter() == 350
    assert o2.getCounter() == 350
    assert o3.getCounter() == 350

    setMockSocket(o1, 1)
    setMockSocket(o2, 1)

    global _g_numSuccessSends
    _g_numSuccessSends = 0

    for i in range(150):
        o1.addValue(1)
    for i in range(200):
        o2.addValue(1)

    doTicks(objs, 10.0, stopFunc=lambda: o1.getCounter() == 700 and o2.getCounter() == 700)

    assert o1.getCounter() == 700
    assert o2.getCounter() == 700

    o1._destroy()
    o2._destroy()
    o3._destroy()


def test_filterParners():
    random.seed(42)

    a = [getNextAddr(), getNextAddr()]

    o1 = TestObj(a[0], [a[1], a[0]])
    assert len(o1._SyncObj__otherNodes) == 1
