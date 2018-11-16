from __future__ import print_function
import hashlib
import io
import math
import os
import time
import pytest
import random
import threading
import sys
import pysyncobj.pickle as pickle
if sys.version_info >= (3, 0):
	xrange = range
from functools import partial
import functools
import struct
import logging
from pysyncobj import SyncObj, SyncObjConf, replicated, FAIL_REASON, _COMMAND_TYPE, \
	createJournal, HAS_CRYPTO, replicated_sync, Utility, SyncObjException, SyncObjConsumer, _RAFT_STATE
from pysyncobj.batteries import ReplCounter, ReplList, ReplDict, ReplSet, ReplLockManager, ReplQueue, ReplPriorityQueue
import pysyncobj.journal
from pysyncobj.node import TCPNode
from collections import defaultdict

logging.basicConfig(format = u'[%(asctime)s %(filename)s:%(lineno)d %(levelname)s]  %(message)s', level = logging.DEBUG)

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
	NOFLUSH_NOVOTE_1 = 8
	NOFLUSH_NOVOTE_2 = 9

class TestObj(SyncObj):

	def __init__(self, selfNodeAddr, otherNodeAddrs,
				 testType = TEST_TYPE.DEFAULT,
				 compactionMinEntries = 0,
				 dumpFile = None,
				 journalFile = None,
				 password = None,
				 dynamicMembershipChange = False,
				 useFork = True,
				 testBindAddr = False,
				 consumers = None,
				 onStateChanged = None,
				 leaderFallbackTimeout = None):

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
			#cfg.appendEntriesBatchSizeBytes = 2 ** 13

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

		if testType in (TEST_TYPE.NOFLUSH_NOVOTE_1, TEST_TYPE.NOFLUSH_NOVOTE_2):
			cfg.logCompactionMinTime = 999999
			cfg.logCompactionMinEntries = 999999
			cfg.appendEntriesPeriod = 0.02
			cfg.raftMinTimeout = 0.1 if testType == TEST_TYPE.NOFLUSH_NOVOTE_1 else 0.48
			cfg.raftMaxTimeout = 0.1000001 if testType == TEST_TYPE.NOFLUSH_NOVOTE_1 else 0.4800001
			cfg.fullDumpFile = dumpFile
			cfg.journalFile = journalFile
			cfg.flushJournal = testType == TEST_TYPE.NOFLUSH_NOVOTE_1

		super(TestObj, self).__init__(selfNodeAddr, otherNodeAddrs, cfg, consumers)
		self.__counter = 0
		self.__data = {}

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

def utilityTickFunc(args, currRes, key, timeToTick):
	u = Utility(args)
	currTime = time.time()
	finishTime = currTime + timeToTick
	while time.time() < finishTime:
		if u.getResult() is not None:
			currRes[key] = u.getResult()
			break

def doSyncObjAdminTicks(objects, arguments, timeToTick, currRes, interval = 0.05, stopFunc=None):
	objThreads = []
	utilityThreads = []
	for o in objects:
		t1= threading.Thread(target=singleTickFunc, args=(o, timeToTick, interval, stopFunc))
		t1.start()
		objThreads.append(t1)
		if arguments.get(o) is not None:
			t2 = threading.Thread(target=utilityTickFunc, args=(arguments[o], currRes, o, timeToTick))
			t2.start()
			utilityThreads.append(t2)
	for t in objThreads:
		t.join()
	for t in utilityThreads:
		t.join()

def doTicks(objects, timeToTick, interval = 0.05, stopFunc = None):
	threads = []
	for o in objects:
		t = threading.Thread(target=singleTickFunc, args=(o, timeToTick, interval, stopFunc))
		t.start()
		threads.append(t)
	for t in threads:
		t.join()

def doAutoTicks(interval = 0.05, stopFunc = None):
	deadline = time.time() + interval
	while not stopFunc():
		time.sleep(0.02)
		t2 = time.time()
		if t2 >= deadline:
			break

_g_nextAddress = 6000 + 60 * (int(time.time()) % 600)

def getNextAddr(ipv6 = False, isLocalhost = False):
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

def test_singleObject():

	random.seed(42)

	a = [getNextAddr(),]

	o1 = TestObj(a[0], [])
	objs = [o1,]

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

def sync_noflush_novote(journalFile2Enabled):
	# Test that if flushing is disabled, the node won't vote until the maximum timeout has been exceeded
	# o1's timeout is set to 0.1 seconds, so it will call for an election almost immediately.
	# o2's timeout is set to 0.48 seconds, so it will not call for an election before o1, and it will ignore o1's request_vote messages.
	# Specifically, o2 is expected to ignore the messages until 1.1 * timeout, i.e. including the one sent by o1 after 0.5 seconds.
	# This test doesn't actually verify that o2 gets o1's request_vote messages, but that should be covered by other tests.
	# Note that o1 has flushing enabled but o2 doesn't!

	random.seed(42)

	a = [getNextAddr(), getNextAddr()]
	if journalFile2Enabled:
		journalFiles = [getNextJournalFile(), getNextJournalFile()]
	else:
		journalFiles = [getNextJournalFile()]
	removeFiles(journalFiles)

	# Make sure that o1 already has a log; this means that it will never accept a vote request from o2
	o1 = TestObj(a[0], [], TEST_TYPE.NOFLUSH_NOVOTE_1, journalFile = journalFiles[0])
	doTicks([o1], 10, stopFunc=lambda: o1.isReady())
	o1.addValue(42)
	doTicks([o1], 10, stopFunc=lambda: o1.getCounter() == 42)
	o1._destroy()

	states = defaultdict(list)

	o1 = TestObj(a[0], [a[1]], TEST_TYPE.NOFLUSH_NOVOTE_1, journalFile = journalFiles[0], onStateChanged=lambda old, new: states[a[0]].append(new))
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.NOFLUSH_NOVOTE_2, journalFile = journalFiles[1] if journalFile2Enabled else None, onStateChanged=lambda old, new: states[a[1]].append(new))
	objs = [o1, o2]

	assert not o1._isReady()
	assert not o2._isReady()

	doTicks(objs, 0.45) #, stopFunc=lambda: o1._SyncObj__raftState == _RAFT_STATE.CANDIDATE)

	# Here, o1 has called several elections, but o2 never granted its vote.

	assert o1._SyncObj__raftState == _RAFT_STATE.CANDIDATE
	assert o2._SyncObj__raftState == _RAFT_STATE.FOLLOWER
	assert _RAFT_STATE.LEADER not in states[a[0]]
	assert states[a[1]] == [] # Never had a state change, i.e. it's still the default follower

	doTicks(objs, 0.1)

	# We have now surpassed o2's timeout, but the last vote request from o1 was at 0.5, i.e. *before* o2's 1.1 * timeout has expired.
	# o2 is expected to have called for an election, but o1 would never vote for o2 due to the missing log entry.

	assert o1._SyncObj__raftState == _RAFT_STATE.CANDIDATE
	assert o2._SyncObj__raftState == _RAFT_STATE.CANDIDATE
	assert _RAFT_STATE.LEADER not in states[a[0]]
	assert _RAFT_STATE.LEADER not in states[a[1]]

	doTicks(objs, 0.1)

	# o1 called for another election at 0.6, i.e. after o2's vote block timeout, so it should now be elected.
	assert o1._SyncObj__raftState == _RAFT_STATE.LEADER
	assert o2._SyncObj__raftState == _RAFT_STATE.FOLLOWER
	assert _RAFT_STATE.LEADER not in states[a[1]]

	o1._destroy()
	o2._destroy()
	removeFiles(journalFiles)

def test_sync_noflush_novote():
	sync_noflush_novote(True) # Test with an unflushed journal file
	sync_noflush_novote(False) # Test with a memory journal

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

	doTicks(objs, 10, stopFunc=lambda: o1.getCounter() == 1000 and \
									   o2.getCounter() == 1000 and \
									   o3.getCounter() == 1000)

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

	doTicks(newObjs, 10, stopFunc=lambda: o1.getCounter() == 2000 and \
										  o2.getCounter() == 2000)

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

	o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile = dumpFiles[0], useFork = useFork)
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile = dumpFiles[1], useFork = useFork)
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
	o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile = dumpFiles[0], useFork = useFork)
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile = dumpFiles[1], useFork = useFork)
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

	o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile = dumpFiles[0])
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile = dumpFiles[1])
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
	o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile = dumpFiles[0])
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile = dumpFiles[1])
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

	o1._destroy()
	o2._destroy()


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

	random.seed(12)

	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]], TEST_TYPE.RAND_1, journalFile=journalFiles[0])
	o2 = TestObj(a[1], [a[2], a[0]], TEST_TYPE.RAND_1, journalFile=journalFiles[1])
	o3 = TestObj(a[2], [a[0], a[1]], TEST_TYPE.RAND_1, journalFile=journalFiles[2])
	objs = [o1, o2, o3]

	st = time.time()
	while time.time() - st < 120.0:
		doTicks(objs, random.random() * 0.3, interval=0.05)
		assert _checkSameLeader(objs)
		assert _checkSameLeader2(objs)
		for i in xrange(0, random.randint(0, 2)):
			random.choice(objs).addValue(random.randint(0, 10))
		newObjs = list(objs)
		newObjs.pop(random.randint(0, len(newObjs) - 1))
		doTicks(newObjs, random.random() * 0.3, interval=0.05)
		assert _checkSameLeader(objs)
		assert _checkSameLeader2(objs)
		for i in xrange(0, random.randint(0, 2)):
			random.choice(objs).addValue(random.randint(0, 10))

	if not (o1.getCounter() == o2.getCounter() == o3.getCounter()):
		print(time.time(), 'counters:', o1.getCounter(), o2.getCounter(), o3.getCounter())
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

	o1 = TestObj(a[0], [a[1], a[2]], dumpFile = dumpFiles[0])
	o2 = TestObj(a[1], [a[2], a[0]], dumpFile = dumpFiles[1])
	o3 = TestObj(a[2], [a[0], a[1]], dumpFile = dumpFiles[2])
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


def __checkParnerNodeExists(obj, nodeAddr, shouldExist = True):
	nodeAddrSet = {node.address for node in obj._SyncObj__otherNodes}
	return (nodeAddr in nodeAddrSet) == shouldExist # either nodeAddr is in nodeAddrSet and shouldExist is True, or nodeAddr isn't in the set and shouldExist is False


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
	o2._addNodeToCluster(a[3])

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
		o2._addNodeToCluster(a[3])

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


	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]], TEST_TYPE.JOURNAL_1, dumpFile = dumpFiles[0], journalFile=journalFiles[0])
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.JOURNAL_1, dumpFile = dumpFiles[1], journalFile=journalFiles[1])
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
	o1 = TestObj(a[0], [a[1]], TEST_TYPE.JOURNAL_1, dumpFile = dumpFiles[0], journalFile=journalFiles[0])
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.JOURNAL_1, dumpFile = dumpFiles[1], journalFile=journalFiles[1])
	objs = [o1, o2]
	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady() and\
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

def test_journalTest2():

	journalFiles = [getNextJournalFile()]
	removeFiles(journalFiles)

	removeFiles(journalFiles)
	journal = createJournal(journalFiles[0], True)
	journal.add(b'cmd1', 1, 0)
	journal.add(b'cmd2', 2, 0)
	journal.add(b'cmd3', 3, 0)
	journal.currentTerm = 42
	journal.votedForNodeId = 'example.org'
	journal._destroy()

	journal = createJournal(journalFiles[0], True)
	assert len(journal) == 3
	assert journal[0] == (b'cmd1', 1, 0)
	assert journal[-1] == (b'cmd3', 3, 0)
	assert journal.currentTerm == 42
	assert journal.votedForNodeId == 'example.org'
	journal.deleteEntriesFrom(2)
	journal.set_currentTerm_and_votedForNodeId(100, 'other.example.net')
	journal._destroy()

	journal = createJournal(journalFiles[0], True)
	assert len(journal) == 2
	assert journal[0] == (b'cmd1', 1, 0)
	assert journal[-1] == (b'cmd2', 2, 0)
	assert journal.currentTerm == 100
	assert journal.votedForNodeId == 'other.example.net'
	journal.deleteEntriesTo(1)
	journal._destroy()

	journal = createJournal(journalFiles[0], True)
	assert len(journal) == 1
	assert journal[0] == (b'cmd2', 2, 0)
	journal._destroy()
	removeFiles(journalFiles)


def test_journal_flushing():
	# Patch ResizableFile to simulate a file which isn't flushed without calling flush explicitly; the read method will always return the expected data, but it won't be written to disk.
	# This is done because testing actual flushing is very difficult from this level. However, it achieves the same effect and tests whether the FileJournal itself handles flushing correctly.
	# Note that this does *not* test whether ResizableFile flushing is correct.

	class TestResizableFile(object):
		# A class that provides the same API as pysyncobj.journal.ResizableFile but never writes data to disk unless it's explicitly flushed.
		# To achieve this, it simply uses an in-memory buffer that is written to disk upon flush. This is to avoid having nearly identical code to ResizableFile in here.

		def __init__(self, filename, defaultContent = None, **kwargs):
			initialData = defaultContent
			if os.path.exists(filename):
				with open(filename, 'rb') as fp:
					initialData = fp.read()
			self._buffer = io.BytesIO(initialData)
			self._filename = filename

		def write(self, offset, data):
			self._buffer.seek(offset)
			self._buffer.write(data)

		def read(self, offset, size):
			self._buffer.seek(offset)
			return self._buffer.read(size)

		def flush(self):
			with open(self._filename, 'wb') as fp:
				fp.write(self._buffer.getvalue())

		def _destroy(self):
			self.flush()
			self._buffer.close()

	origResizableFile = pysyncobj.journal.ResizableFile
	try:
		pysyncobj.journal.ResizableFile = TestResizableFile

		journalFiles = [getNextJournalFile()]

		def run_test(flushJournal, useDestroy):
			for mode in ('add', 'clear', 'deleteEntriesFrom', 'deleteEntriesTo', 'currentTerm', 'votedForNodeId', 'currentTerm and votedForNodeId'):
				removeFiles(journalFiles)
				journal = createJournal(journalFiles[0], flushJournal)
				assert len(journal) == 0
				journal.add(b'cmd1', 1, 0)
				journal.add(b'cmd2', 2, 0)
				journal.add(b'cmd3', 3, 0)
				expectedCurrentTermNotWorking = 13
				journal.currentTerm = 13
				expectedVotedForNodeIdNotWorking = 'bad.example.org'
				journal.votedForNodeId = 'bad.example.org'
				journal.flush()
				expectedNotWorking = [(b'cmd1', 1, 0), (b'cmd2', 2, 0), (b'cmd3', 3, 0)]

				# Default values for when something else is being tested
				expectedCurrentTermWorking = expectedCurrentTermNotWorking
				expectedVotedForNodeIdWorking = expectedVotedForNodeIdNotWorking
				expectedWorking = expectedNotWorking

				if mode == 'add':
					journal.add(b'cmd4', 4, 0)
					expectedWorking = [(b'cmd1', 1, 0), (b'cmd2', 2, 0), (b'cmd3', 3, 0), (b'cmd4', 4, 0)]
				elif mode == 'clear':
					journal.clear()
					expectedWorking = []
				elif mode == 'deleteEntriesFrom':
					journal.deleteEntriesFrom(2)
					expectedWorking = [(b'cmd1', 1, 0), (b'cmd2', 2, 0)]
				elif mode == 'deleteEntriesTo':
					journal.deleteEntriesTo(2)
					expectedWorking = [(b'cmd3', 3, 0)]
				elif mode == 'currentTerm':
					journal.currentTerm = 42
					expectedCurrentTermWorking = 42
				elif mode == 'votedForNodeId':
					journal.votedForNodeId = 'good.example.org'
					expectedVotedForNodeIdWorking = 'good.example.org'
				elif mode == 'currentTerm and votedForNodeId':
					journal.set_currentTerm_and_votedForNodeId(42, 'good.example.org')
					expectedCurrentTermWorking = 42
					expectedVotedForNodeIdWorking = 'good.example.org'

				if useDestroy:
					journal._destroy()
				del journal

				journal = createJournal(journalFiles[0], flushJournal)
				if flushJournal or useDestroy:
					assert journal[:] == expectedWorking
					assert journal.currentTerm == expectedCurrentTermWorking
					assert journal.votedForNodeId == expectedVotedForNodeIdWorking
				else:
					assert journal[:] == expectedNotWorking
					assert journal.currentTerm == expectedCurrentTermNotWorking
					assert journal.votedForNodeId == expectedVotedForNodeIdNotWorking

		# Verify that, when journal flushing is disabled, values written after the last flush without using _destroy (which implicitly flushes) are not preserved.
		run_test(False, False)

		# Verify that using _destroy causes a flush
		run_test(False, True)

		# Verify that journal flushing without _destroy preserves everything
		run_test(True, False)

		# Verify that journal flushing with _destroy works as well
		run_test(True, True)
	finally:
		pysyncobj.journal.ResizableFile = origResizableFile

	removeFiles(journalFiles)


def test_journal_upgrade_version_1_to_2():
	def data_to_entry(index, term, data):
		d = struct.pack('<QQ', index, term) + data
		l = struct.pack('<I', len(d))
		return l + d + l

	# Base header including app name, version, and journal format version; notably, this does *not* include the last record offset
	# Note that the name and version strings here are intentionally modified to check whether they're replaced on migration as expected.
	baseHeader = (
		b'PYSYNCOBJ!' + b'\x00' * 14 +
		b'0.3.0' + b'\x00' * 3 +
		b'\x01\x00\x00\x00'
	  )

	journals = [
		# (name, journal entry bytes (including record size but still without the last record offset!), expected journal entries)

		('empty journal', b'', []),

		(
		  'one proper entry',
		  data_to_entry(1, 1, b'\x01'),
		  [(b'\x01', 1, 1)]
		),

		# An entry with arbitrary data (the journal shouldn't do any parsing of the data)
		(
		  'one arbitrary',
		  data_to_entry(1, 1, b'testing the journal'),
		  [(b'testing the journal', 1, 1)]
		),

		# Really arbitrary...
		(
		  'one really arbitrary',
		  data_to_entry(1, 1, b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f !"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\x7f\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff'),
		  [(bytes(bytearray.fromhex(''.join('{:02x}'.format(i) for i in range(256)))), 1, 1)] # bytes(range(256)) on Py3
		),

		# Empty data should work as well
		(
		  'empty data',
		  data_to_entry(1, 1, b''),
		  [(b'', 1, 1)]
		),

		# A few entries
		(
		  'a few entries',
		  data_to_entry(1, 1, b'\x01') + data_to_entry(2, 2, b'\x01') + data_to_entry(3, 4, b'\x01'),
		  [(b'\x01', 1, 1), (b'\x01', 2, 2), (b'\x01', 3, 4)]
		),
	]

	# Generate larger journals
	# 400 B: some arbitrary length which fits well within a single block
	# 1000 B: should expand to exactly 1024 B in the new format, i.e. no resizing of the file necessary
	# 1020/3/4 B: almost or entirely full journal which can't fit the new header fields anymore, so it needs to be expanded on migration
	# 1025 B: a journal which already has been expanded
	# pi MiB: some large journal
	for length in (400, 1000, 1020, 1023, 1024, 1025, int(math.pi * 1048576)): # length including the header...
		for smallRecords in (True, False):
			entries = []
			expectedEntries = []
			deltaLength = length - len(baseHeader) - 4  # 4: last record offset

			# Each record has an overhead of 8 bytes (length of the record at beginning and end) and also stores an index and a term with 8 bytes each.
			if smallRecords:
				# Small records hold only 1 B of data, i.e. are 25 B in total; so we create deltaLength // 25 - 1 such records and then fill up with one possibly slightly larger record (up to 25 bytes of payload)
				nSmall = deltaLength // 25 - 1
				for i in range(nSmall):
					entries.append(data_to_entry(i, i, b'\x01'))
					expectedEntries.append((b'\x01', i, i))
				remainingData = b'\x00' * (deltaLength - nSmall * 25 - 24)
				entries.append(data_to_entry(nSmall, nSmall, remainingData))
				expectedEntries.append((remainingData, nSmall, nSmall))
			else:
				# Just one huge record to fit the length
				data = b'\x00' * (deltaLength - 24)
				entries.append(data_to_entry(1, 1, data))
				expectedEntries.append((data, 1, 1))

			journals.append(('length={} small={}'.format(length, smallRecords), b''.join(entries), expectedEntries))

	journalFile = getNextJournalFile()

	for name, entryBytes, expectedEntries in journals:
		print(name)
		removeFiles([journalFile])
		with open(journalFile, 'wb') as fp:
			size = 0
			fp.write(baseHeader)
			size += len(baseHeader)
			fp.write(struct.pack('<I', len(baseHeader) + 4 + len(entryBytes)))
			size += 4
			fp.write(entryBytes)
			size += len(entryBytes)

			# Fill up with zero bytes to a power of 2
			fp.write(b'\x00' * (2 ** max(math.ceil(math.log(size, 2)), 10) - size))

		journal = createJournal(journalFile, True)
		assert journal[:] == expectedEntries
		journal._destroy()
		assert os.path.getsize(journalFile) == 2 ** max(math.ceil(math.log(size + 24, 2)), 10)
		with open(journalFile, 'rb') as fp:
			# Check app name, version, and journal format version
			assert fp.read(24) == b'PYSYNCOBJ' + b'\x00' * 15
			assert fp.read(8) != b'0.3.0' + b'\x00' * 3
			assert fp.read(4) == b'\x02\x00\x00\x00'
			fp.seek(60)
			assert struct.unpack('<I', fp.read(4))[0] == size + 24
			fp.seek(size + 24)
			assert fp.read().replace(b'\x00', b'') == b''

	removeFiles([journalFile])


def test_journal_voted_for_proxy():
	nodeId = 'example.org'
	otherNodeId = 'example.net'

	for votedFor in [
	  pysyncobj.journal.VotedForNodeIdHashProxy(nodeId),
	  pysyncobj.journal.VotedForNodeIdHashProxy(nodeId = nodeId),
	  pysyncobj.journal.VotedForNodeIdHashProxy(_hash = hashlib.md5(pickle.dumps(nodeId)).digest()),
	 ]:
		assert votedFor == nodeId
		assert nodeId == votedFor
		assert votedFor != otherNodeId
		assert otherNodeId != votedFor
		assert votedFor is not None


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

	o1 = TestObj(a[0], [a[1]], TEST_TYPE.LARGE_COMMAND, dumpFile = dumpFiles[0], leaderFallbackTimeout=60.0)
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.LARGE_COMMAND, dumpFile = dumpFiles[1], leaderFallbackTimeout=60.0)
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
	o1 = TestObj(a[0], [a[1]], TEST_TYPE.LARGE_COMMAND, dumpFile = dumpFiles[0], leaderFallbackTimeout=60.0)
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.LARGE_COMMAND, dumpFile = dumpFiles[1], leaderFallbackTimeout=60.0)
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

def test_readOnlyNodes():

	random.seed(12)

	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]])
	o2 = TestObj(a[1], [a[2], a[0]])
	o3 = TestObj(a[2], [a[0], a[1]])
	objs = [o1, o2, o3]

	b1 = TestObj(None, [a[0], a[1], a[2]])
	b2 = TestObj(None, [a[0], a[1], a[2]])

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

	doTicks(objs + roObjs, 10.0, stopFunc=lambda: sum([int(o.getCounter() == 400) for o in objs + roObjs]) == len(objs + roObjs))

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

	o1._destroy()
	o2._destroy()
	o3._destroy()

	b1._destroy()
	b2._destroy()

def test_syncobjAdminStatus():
	assert HAS_CRYPTO

	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]], password='123')
	o2 = TestObj(a[1], [a[0]], password='123')

	assert not o1._isReady()
	assert not o2._isReady()

	doTicks([o1, o2], 10.0, stopFunc= lambda : o1._isReady() and o2._isReady())

	assert o1._isReady()
	assert o2._isReady()

	status1 = o1._getStatus()
	status2 = o2._getStatus()

	assert 'version' in status1
	assert 'log_len' in status2

	trueRes = {
		o1 : '\n'.join('%s: %s' % (k, v) for k, v in sorted(status1.items())),
		o2 : '\n'.join('%s: %s' % (k, v) for k, v in sorted(status2.items())),
	}

	currRes = {
	}
	args = {
		o1 : ['-conn', a[0], '-pass', '123', '-status'],
		o2 : ['-conn', a[1], '-pass', '123', '-status'],
	}
	doSyncObjAdminTicks([o1, o2], args, 10.0, currRes, stopFunc= lambda : currRes.get(o1) is not None and currRes.get(o2) is not None)

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

	doTicks([o1, o2], 10.0, stopFunc= lambda : o1._isReady() and o2._isReady())

	assert o1._isReady()
	assert o2._isReady()

	trueRes = 'SUCCESS ADD '+ a[2]

	currRes = {}

	args = {
		o1 : ['-conn', a[0], '-add', a[2]],
	}

	doSyncObjAdminTicks([o1, o2], args, 10.0, currRes, stopFunc=lambda :currRes.get(o1) is not None)

	assert currRes[o1] == trueRes

	o3 = TestObj(a[2], [a[1], a[0]], dynamicMembershipChange=True)

	doTicks([o1, o2, o3], 10.0, stopFunc= lambda : o1._isReady() and o2._isReady() and o3._isReady())

	assert o1._isReady()
	assert o2._isReady()
	assert o3._isReady()

	trueRes = 'SUCCESS REMOVE '+ a[2]
	args[o1] = None
	args[o2] = ['-conn', a[1], '-remove', a[2]]

	doSyncObjAdminTicks([o1, o2, o3], args, 10.0, currRes, stopFunc=lambda :currRes.get(o2) is not None)

	assert currRes[o2] == trueRes

	o3._destroy()

	doTicks([o1, o2], 10.0, stopFunc= lambda : o1._isReady() and o2._isReady())

	assert o1._isReady()
	assert o2._isReady()

	o1._destroy()
	o2._destroy()

def test_syncobjAdminSetVersion():

	random.seed(42)

	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]], dynamicMembershipChange=True)
	o2 = TestObj(a[1], [a[0]], dynamicMembershipChange=True)

	assert not o1._isReady()
	assert not o2._isReady()

	doTicks([o1, o2], 10.0, stopFunc= lambda : o1._isReady() and o2._isReady())

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
		o1 : ['-conn', a[0], '-set_version', '1'],
	}

	doSyncObjAdminTicks([o1, o2], args, 10.0, currRes, stopFunc=lambda :currRes.get(o1) is not None)

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
