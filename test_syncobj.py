import os
import time
import random
import threading
import cPickle
from functools import partial
import functools
import struct
from pysyncobj import SyncObj, SyncObjConf, replicated, FAIL_REASON, _COMMAND_TYPE, createJournal, HAS_CRYPTO, replicated_sync

_bchr = functools.partial(struct.pack, 'B')

class TEST_TYPE:
	DEFAULT = 0
	COMPACTION_1 = 1
	COMPACTION_2 = 2
	RAND_1 = 3
	JOURNAL_1 = 4
	AUTO_TICK_1 = 5

class TestObj(SyncObj):

	def __init__(self, selfNodeAddr, otherNodeAddrs,
				 testType = TEST_TYPE.DEFAULT,
				 compactionMinEntries = 0,
				 dumpFile = None,
				 journalFile = None,
				 password = None,
				 dynamicMembershipChange = False,
				 useFork = True):

		cfg = SyncObjConf(autoTick=False, appendEntriesUseBatch=False)
		cfg.appendEntriesPeriod = 0.1
		cfg.raftMinTimeout = 0.5
		cfg.raftMaxTimeout = 1.0
		cfg.dynamicMembershipChange = dynamicMembershipChange

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

		super(TestObj, self).__init__(selfNodeAddr, otherNodeAddrs, cfg)
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

	def getCounter(self):
		return self.__counter

	def getValue(self, key):
		return self.__data.get(key, None)

	def dumpKeys(self):
		print 'keys:', sorted(self.__data.keys())

def singleTickFunc(o, timeToTick, interval, stopFunc):
	currTime = time.time()
	finishTime = currTime + timeToTick
	while time.time() < finishTime:
		o._onTick(interval)
		if stopFunc is not None:
			if stopFunc():
				break

def doTicks(objects, timeToTick, interval = 0.05, stopFunc = None):
	threads = []
	for o in objects:
		t = threading.Thread(target=singleTickFunc, args=(o, timeToTick, interval, stopFunc))
		t.start()
		threads.append(t)
	for t in threads:
		t.join()

_g_nextAddress = 6000 + 60 * (int(time.time()) % 600)

def getNextAddr():
	global _g_nextAddress
	_g_nextAddress += 1
	return 'localhost:%d' % _g_nextAddress

def test_syncTwoObjects():

	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]])
	o2 = TestObj(a[1], [a[0]])
	objs = [o1, o2]

	assert not o1._isReady()
	assert not o2._isReady()

	doTicks(objs, 10.0, stopFunc=lambda: o1._isReady() and o2._isReady())

	o1._printStatus()

	assert o1._getLeader() in a
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

def test_syncThreeObjectsLeaderFail():

	random.seed(12)

	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]])
	o2 = TestObj(a[1], [a[2], a[0]])
	o3 = TestObj(a[2], [a[0], a[1]])
	objs = [o1, o2, o3]

	assert not o1._isReady()
	assert not o2._isReady()
	assert not o3._isReady()

	doTicks(objs, 10.0, stopFunc=lambda: o1._isReady() and o2._isReady() and o3._isReady())

	assert o1._isReady()
	assert o2._isReady()
	assert o3._isReady()

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()
	assert o1._getLeader() == o3._getLeader()

	o1.addValue(150)
	o2.addValue(200)

	doTicks(objs, 10.0, stopFunc=lambda: o3.getCounter() == 350)

	assert o3.getCounter() == 350

	prevLeader = o1._getLeader()

	newObjs = [o for o in objs if o._getSelfNodeAddr() != prevLeader]

	assert len(newObjs) == 2

	doTicks(newObjs, 10.0, stopFunc=lambda: newObjs[0]._getLeader() != prevLeader and \
											newObjs[0]._getLeader() in a and \
											newObjs[0]._getLeader() == newObjs[1]._getLeader())

	assert newObjs[0]._getLeader() != prevLeader
	assert newObjs[0]._getLeader() in a
	assert newObjs[0]._getLeader() == newObjs[1]._getLeader()

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

	assert o1._getLeader() in a
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

	assert o1._getLeader() in a
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
		try:
			os.remove(f)
		except:
			pass

def checkDumpToFile(useFork):
	removeFiles(['dump1.bin', 'dump2.bin'])

	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile = 'dump1.bin', useFork = useFork)
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile = 'dump2.bin', useFork = useFork)
	objs = [o1, o2]
	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

	assert o1._getLeader() in a
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
	o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile = 'dump1.bin', useFork = useFork)
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile = 'dump2.bin', useFork = useFork)
	objs = [o1, o2]
	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())
	assert o1._isReady()
	assert o2._isReady()

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	assert o1.getCounter() == 350
	assert o2.getCounter() == 350

	o1._destroy()
	o2._destroy()

	removeFiles(['dump1.bin', 'dump2.bin'])

def test_checkDumpToFile():
	if hasattr(os, 'fork'):
		checkDumpToFile(True)
	checkDumpToFile(False)


def getRandStr():
	return '%0100000x' % random.randrange(16 ** 100000)


def test_checkBigStorage():
	removeFiles(['dump1.bin', 'dump2.bin'])

	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile = 'dump1.bin')
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile = 'dump2.bin')
	objs = [o1, o2]
	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

	assert o1._getLeader() in a
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
	o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile = 'dump1.bin')
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile = 'dump2.bin')
	objs = [o1, o2]
	# Wait for disk load, election and replication
	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	assert o1.getValue('test') == testRandStr
	assert o2.getValue('test') == testRandStr

	o1._destroy()
	o2._destroy()

	removeFiles(['dump1.bin', 'dump2.bin'])


def test_encryptionCorrectPassword():
	if not HAS_CRYPTO:
		return

	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]], password='asd')
	o2 = TestObj(a[1], [a[0]], password='asd')
	objs = [o1, o2]
	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	o1.addValue(150)
	o2.addValue(200)

	doTicks(objs, 10, stopFunc=lambda: o1.getCounter() == 350 and o2.getCounter() == 350)

	assert o1.getCounter() == 350
	assert o2.getCounter() == 350

	o1._destroy()
	o2._destroy()


def test_encryptionWrongPassword():
	if not HAS_CRYPTO:
		return

	random.seed(12)

	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]], password='asd')
	o2 = TestObj(a[1], [a[2], a[0]], password='asd')
	o3 = TestObj(a[2], [a[0], a[1]], password='qwe')
	objs = [o1, o2, o3]

	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	doTicks(objs, 1.0)
	assert o3._getLeader() is None

	o1._destroy()
	o2._destroy()
	o3._destroy()


def _checkSameLeader(objs):
	for obj1 in objs:
		l1 = obj1._getLeader()
		if l1 != obj1._getSelfNodeAddr():
			continue
		t1 = obj1._getTerm()
		for obj2 in objs:
			l2 = obj2._getLeader()
			if l2 != obj2._getSelfNodeAddr():
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

	removeFiles(['journal1.bin', 'journal2.bin', 'journal3.bin'])

	random.seed(12)

	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]], TEST_TYPE.RAND_1, journalFile='journal1.bin')
	o2 = TestObj(a[1], [a[2], a[0]], TEST_TYPE.RAND_1, journalFile='journal2.bin')
	o3 = TestObj(a[2], [a[0], a[1]], TEST_TYPE.RAND_1, journalFile='journal3.bin')
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
		print time.time(), 'counters:', o1.getCounter(), o2.getCounter(), o3.getCounter()
	st = time.time()
	while not (o1.getCounter() == o2.getCounter() == o3.getCounter()):
		doTicks(objs, 2.0, interval=0.05)
		if time.time() - st > 30:
			break

	if not (o1.getCounter() == o2.getCounter() == o3.getCounter()):
		o1._printStatus()
		o2._printStatus()
		o3._printStatus()
		print 'Logs same:', o1._SyncObj__raftLog == o2._SyncObj__raftLog == o3._SyncObj__raftLog
		print time.time(), 'counters:', o1.getCounter(), o2.getCounter(), o3.getCounter()
		raise AssertionError('Values not equal')

	counter = o1.getCounter()
	o1._destroy()
	o2._destroy()
	o3._destroy()
	del o1
	del o2
	del o3
	time.sleep(0.1)

	o1 = TestObj(a[0], [a[1], a[2]], TEST_TYPE.RAND_1, journalFile='journal1.bin')
	o2 = TestObj(a[1], [a[2], a[0]], TEST_TYPE.RAND_1, journalFile='journal2.bin')
	o3 = TestObj(a[2], [a[0], a[1]], TEST_TYPE.RAND_1, journalFile='journal3.bin')
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
		print 'Logs same:', o1._SyncObj__raftLog == o2._SyncObj__raftLog == o3._SyncObj__raftLog
		print time.time(), 'counters:', o1.getCounter(), o2.getCounter(), o3.getCounter(), counter
		raise AssertionError('Values not equal')

	removeFiles(['journal1.bin', 'journal2.bin', 'journal3.bin'])


# Ensure that raftLog after serialization is the same as in serialized data
def test_logCompactionRegressionTest1():
	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]])
	o2 = TestObj(a[1], [a[0]])
	objs = [o1, o2]

	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

	assert o1._getLeader() in a
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
	removeFiles(['dump1.bin', 'dump2.bin', 'dump3.bin'])

	random.seed(12)

	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]], dumpFile = 'dump1.bin')
	o2 = TestObj(a[1], [a[2], a[0]], dumpFile = 'dump2.bin')
	o3 = TestObj(a[2], [a[0], a[1]], dumpFile = 'dump3.bin')
	objs = [o1, o2]

	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

	objs = [o1, o2, o3]
	o1.addValue(2)
	o1.addValue(3)
	doTicks(objs, 10, stopFunc=lambda: o3.getCounter() == 5)

	o3._forceLogCompaction()
	doTicks(objs, 0.5)

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader() == o3._getLeader()

	o3._destroy()

	objs = [o1, o2]

	o1.addValue(2)
	o1.addValue(3)

	doTicks(objs, 0.5)
	o1._forceLogCompaction()
	o2._forceLogCompaction()
	doTicks(objs, 0.5)

	o3 = TestObj(a[2], [a[0], a[1]], dumpFile='dump3.bin')
	objs = [o1, o2, o3]
	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady() and o3._isReady())

	assert o1._isReady()
	assert o2._isReady()
	assert o3._isReady()

	o1._destroy()
	o2._destroy()
	o3._destroy()

	removeFiles(['dump1.bin', 'dump2.bin', 'dump3.bin'])


def __checkParnerNodeExists(obj, nodeName, shouldExist = True):
	nodesSet1 = set()
	nodesSet2 = set(obj._SyncObj__otherNodesAddrs)
	for node in obj._SyncObj__nodes:
		nodesSet1.add(node.getAddress())

	if nodesSet1 != nodesSet2:
		print 'otherNodes:', nodesSet2
		print 'nodes:', nodesSet1

	assert nodesSet1 == nodesSet2
	if shouldExist:
		assert nodeName in nodesSet1
	else:
		assert nodeName not in nodesSet1

def test_doChangeClusterUT1():
	removeFiles(['dump1.bin'])

	baseAddr = getNextAddr()
	oterAddr = getNextAddr()

	o1 = TestObj(baseAddr, ['localhost:1235', oterAddr], dumpFile='dump1.bin', dynamicMembershipChange=True)
	__checkParnerNodeExists(o1, 'localhost:1238', False)
	__checkParnerNodeExists(o1, 'localhost:1239', False)
	__checkParnerNodeExists(o1, 'localhost:1235', True)

	noop = _bchr(_COMMAND_TYPE.NO_OP)
	member = _bchr(_COMMAND_TYPE.MEMBERSHIP)

	# Check regular configuration change - adding
	o1._onMessageReceived('localhost:12345', {
		'type': 'append_entries',
		'term': 1,
		'prevLogIdx': 1,
		'prevLogTerm': 0,
		'commit_index': 2,
		'entries': [(noop, 2, 1), (noop, 3, 1), (member + cPickle.dumps(['add', 'localhost:1238']), 4, 1)]
	})
	__checkParnerNodeExists(o1, 'localhost:1238', True)
	__checkParnerNodeExists(o1, 'localhost:1239', False)

	# Check rollback adding
	o1._onMessageReceived('localhost:1236', {
		'type': 'append_entries',
		'term': 2,
		'prevLogIdx': 2,
		'prevLogTerm': 1,
		'commit_index': 3,
		'entries': [(noop, 3, 2), (member + cPickle.dumps(['add', 'localhost:1239']), 4, 2)]
	})
	__checkParnerNodeExists(o1, 'localhost:1238', False)
	__checkParnerNodeExists(o1, 'localhost:1239', True)
	__checkParnerNodeExists(o1, oterAddr, True)

	# Check regular configuration change - removing
	o1._onMessageReceived('localhost:1236', {
		'type': 'append_entries',
		'term': 2,
		'prevLogIdx': 4,
		'prevLogTerm': 2,
		'commit_index': 4,
		'entries': [(member + cPickle.dumps(['rem', 'localhost:1235']), 5, 2)]
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


def test_doChangeClusterUT2():
	a = [getNextAddr(), getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]], dynamicMembershipChange=True)
	o2 = TestObj(a[1], [a[2], a[0]], dynamicMembershipChange=True)
	o3 = TestObj(a[2], [a[0], a[1]], dynamicMembershipChange=True)

	doTicks([o1, o2, o3], 10, stopFunc=lambda: o1._isReady() and o2._isReady() and o3._isReady())

	assert o1._isReady() == o2._isReady() == o3._isReady() == True
	o3.addValue(50)
	o2._addNodeToCluster(a[3])
	doTicks([o1, o2, o3], 3.5)
	__checkParnerNodeExists(o1, a[3], True)
	__checkParnerNodeExists(o2, a[3], True)
	__checkParnerNodeExists(o3, a[3], True)
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
	removeFiles(['dump1.bin', 'dump2.bin', 'journal1.bin', 'journal2.bin'])

	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]], TEST_TYPE.JOURNAL_1, dumpFile = 'dump1.bin', journalFile='journal1.bin')
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.JOURNAL_1, dumpFile = 'dump2.bin', journalFile='journal2.bin')
	objs = [o1, o2]
	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	o1.addValue(150)
	o2.addValue(200)

	doTicks(objs, 10, stopFunc=lambda: o1.getCounter() == 350 and o2.getCounter() == 350)

	assert o1.getCounter() == 350
	assert o2.getCounter() == 350

	o1._destroy()
	o2._destroy()

	a = [getNextAddr(), getNextAddr()]
	o1 = TestObj(a[0], [a[1]], TEST_TYPE.JOURNAL_1, dumpFile = 'dump1.bin', journalFile='journal1.bin')
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.JOURNAL_1, dumpFile = 'dump2.bin', journalFile='journal2.bin')
	objs = [o1, o2]
	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady() and\
									   o1.getCounter() == 350 and o2.getCounter() == 350)
	assert o1._isReady()
	assert o2._isReady()

	assert o1._getLeader() in a
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
	o1 = TestObj(a[0], [a[1]], TEST_TYPE.JOURNAL_1, dumpFile='dump1.bin', journalFile='journal1.bin')
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.JOURNAL_1, dumpFile='dump2.bin', journalFile='journal2.bin')
	objs = [o1, o2]
	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady() and \
									   o1.getCounter() == 900 and o2.getCounter() == 900)
	assert o1._isReady()
	assert o2._isReady()

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	assert o1.getCounter() == 900
	assert o2.getCounter() == 900

	o1._destroy()
	o2._destroy()

	removeFiles(['dump1.bin', 'dump2.bin', 'journal1.bin', 'journal2.bin'])

def test_journalTest2():
	removeFiles(['journal.bin'])
	journal = createJournal('journal.bin')
	journal.add(b'cmd1', 1, 0)
	journal.add(b'cmd2', 2, 0)
	journal.add(b'cmd3', 3, 0)
	journal._destroy()

	journal = createJournal('journal.bin')
	assert len(journal) == 3
	assert journal[0] == (b'cmd1', 1, 0)
	assert journal[-1] == (b'cmd3', 3, 0)
	journal.deleteEntriesFrom(2)
	journal._destroy()

	journal = createJournal('journal.bin')
	assert len(journal) == 2
	assert journal[0] == (b'cmd1', 1, 0)
	assert journal[-1] == (b'cmd2', 2, 0)
	journal.deleteEntriesTo(1)
	journal._destroy()

	journal = createJournal('journal.bin')
	assert len(journal) == 1
	assert journal[0] == (b'cmd2', 2, 0)
	journal._destroy()
	removeFiles(['journal.bin'])

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

	assert o1._getLeader() in a
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
	removeFiles(['dump1.bin', 'dump2.bin'])

	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile = 'dump1.bin')
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile = 'dump2.bin')
	objs = [o1, o2]
	doTicks(objs, 10, stopFunc=lambda: o1._isReady() and o2._isReady())

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	# Generate ~20Mb data.
	testRandStr = getRandStr()
	bigStr = ''
	for i in xrange(0, 200):
		bigStr += getRandStr()
	o1.addKeyValue('big', bigStr)
	o1.addKeyValue('test', testRandStr)

	# Wait for replication.
	doTicks(objs, 10, stopFunc=lambda: o1.getValue('test') == testRandStr and \
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
	o1 = TestObj(a[0], [a[1]], TEST_TYPE.COMPACTION_2, dumpFile = 'dump1.bin')
	o2 = TestObj(a[1], [a[0]], TEST_TYPE.COMPACTION_2, dumpFile = 'dump2.bin')
	objs = [o1, o2]
	# Wait for disk load, election and replication

	doTicks(objs, 10, stopFunc=lambda: o1.getValue('test') == testRandStr and \
									   o2.getValue('test') == testRandStr and \
									   o1.getValue('big') == bigStr and \
									   o2.getValue('big') == bigStr and \
									   o1._isReady() and o2._isReady())

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	assert o1.getValue('test') == testRandStr
	assert o2.getValue('big') == bigStr
	assert o1.getValue('test') == testRandStr
	assert o2.getValue('big') == bigStr

	o1._destroy()
	o2._destroy()

	removeFiles(['dump1.bin', 'dump2.bin'])

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

	o1._printStatus()
	o2._printStatus()
	o3._printStatus()

	b1._printStatus()

	assert b1.getCounter() == b2.getCounter() == 350
	assert o1._getLeader() == b1._getLeader() == o2._getLeader() == b2._getLeader()
	assert b1._getLeader() in a

	o1._destroy()
	o2._destroy()
	o3._destroy()

	b1._destroy()
	b2._destroy()
