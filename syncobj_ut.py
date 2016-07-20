import os
import time
import random
import threading
import sys
from functools import partial
from pysyncobj import SyncObj, SyncObjConf, replicated, FAIL_REASON

class TestObj(SyncObj):

	def __init__(self, selfNodeAddr, otherNodeAddrs,
				 compactionTest = 0,
				 dumpFile = None,
				 compactionTest2 = False,
				 password = None,
				 randTest = False):

		cfg = SyncObjConf(autoTick=False, appendEntriesUseBatch=False)
		cfg.appendEntriesPeriod = 0.1
		cfg.raftMinTimeout = 0.5
		cfg.raftMaxTimeout = 1.0
		if compactionTest:
			cfg.logCompactionMinEntries = compactionTest
			cfg.logCompactionMinTime = 0.1
			cfg.appendEntriesUseBatch = True
			cfg.fullDumpFile = dumpFile
		if compactionTest2:
			cfg.logCompactionMinEntries = 99999
			cfg.logCompactionMinTime = 99999
			cfg.fullDumpFile = dumpFile
		if password is not None:
			cfg.password = password
		if randTest:
			cfg.autoTickPeriod = 0.05
			cfg.appendEntriesPeriod = 0.02
			cfg.raftMinTimeout = 0.1
			cfg.raftMaxTimeout = 0.2
			cfg.logCompactionMinTime = 9999999
			cfg.logCompactionMinEntries = 9999999

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

	def getCounter(self):
		return self.__counter

	def getValue(self, key):
		return self.__data.get(key, None)

	def dumpKeys(self):
		print 'keys:', sorted(self.__data.keys())

def singleTickFunc(o, timeToTick, interval):
	currTime = time.time()
	finishTime = currTime + timeToTick
	while time.time() < finishTime:
		o._onTick(interval)

def doTicks(objects, timeToTick, interval = 0.05):
	threads = []
	for o in objects:
		t = threading.Thread(target=singleTickFunc, args=(o, timeToTick, interval))
		t.start()
		threads.append(t)
	for t in threads:
		t.join()

_g_nextAddress = 6000 + 60 * (int(time.time()) % 600)

def getNextAddr():
	global _g_nextAddress
	_g_nextAddress += 1
	return 'localhost:%d' % _g_nextAddress

def syncTwoObjects():

	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]])
	o2 = TestObj(a[1], [a[0]])
	objs = [o1, o2]

	assert not o1._isReady()
	assert not o2._isReady()

	doTicks(objs, 4.5)

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()
	assert o1._isReady()
	assert o2._isReady()

	o1.addValue(150)
	o2.addValue(200)

	doTicks(objs, 1.5)

	assert o1._isReady()
	assert o2._isReady()

	assert o1.getCounter() == 350
	assert o2.getCounter() == 350

def syncThreeObjectsLeaderFail():

	random.seed(12)

	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]])
	o2 = TestObj(a[1], [a[2], a[0]])
	o3 = TestObj(a[2], [a[0], a[1]])
	objs = [o1, o2, o3]

	assert not o1._isReady()
	assert not o2._isReady()
	assert not o3._isReady()

	doTicks(objs, 4.5)

	assert o1._isReady()
	assert o2._isReady()
	assert o3._isReady()

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()
	assert o1._getLeader() == o3._getLeader()

	o1.addValue(150)
	o2.addValue(200)

	doTicks(objs, 1.5)

	assert o3.getCounter() == 350

	prevLeader = o1._getLeader()

	newObjs = [o for o in objs if o._getSelfNodeAddr() != prevLeader]

	assert len(newObjs) == 2

	doTicks(newObjs, 4.5)
	assert newObjs[0]._getLeader() != prevLeader
	assert newObjs[0]._getLeader() in a
	assert newObjs[0]._getLeader() == newObjs[1]._getLeader()

	newObjs[1].addValue(50)

	doTicks(newObjs, 1.5)

	assert newObjs[0].getCounter() == 400

	doTicks(objs, 4.5)
	for o in objs:
		assert o.getCounter() == 400

def manyActionsLogCompaction():

	random.seed(42)

	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]], compactionTest=100)
	o2 = TestObj(a[1], [a[2], a[0]], compactionTest=100)
	o3 = TestObj(a[2], [a[0], a[1]], compactionTest=100)
	objs = [o1, o2, o3]

	assert not o1._isReady()
	assert not o2._isReady()
	assert not o3._isReady()

	doTicks(objs, 4.5)

	assert o1._isReady()
	assert o2._isReady()
	assert o3._isReady()

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()
	assert o1._getLeader() == o3._getLeader()

	for i in xrange(0, 500):
		o1.addValue(1)
		o2.addValue(1)

	doTicks(objs, 1.5)

	assert o1.getCounter() == 1000
	assert o2.getCounter() == 1000
	assert o3.getCounter() == 1000

	assert o1._getRaftLogSize() <= 100
	assert o2._getRaftLogSize() <= 100
	assert o3._getRaftLogSize() <= 100

	newObjs = [o1, o2]
	doTicks(newObjs, 4.5)

	for i in xrange(0, 500):
		o1.addValue(1)
		o2.addValue(1)

	doTicks(newObjs, 4.0)

	assert o1.getCounter() == 2000
	assert o2.getCounter() == 2000
	assert o3.getCounter() != 2000

	doTicks(objs, 4.5)

	assert o3.getCounter() == 2000

	assert o1._getRaftLogSize() <= 100
	assert o2._getRaftLogSize() <= 100
	assert o3._getRaftLogSize() <= 100

def onAddValue(res, err, info):
	assert res == 3
	assert err == FAIL_REASON.SUCCESS
	info['callback'] = True

def checkCallbacksSimple():

	random.seed(42)

	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]])
	o2 = TestObj(a[1], [a[2], a[0]])
	o3 = TestObj(a[2], [a[0], a[1]])
	objs = [o1, o2, o3]

	assert not o1._isReady()
	assert not o2._isReady()
	assert not o3._isReady()

	doTicks(objs, 4.5)

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

	doTicks(objs, 1.5)

	assert o2.getCounter() == 3
	assert callbackInfo['callback'] == True

def removeFiles(files):
	for f in (files):
		try:
			os.remove(f)
		except:
			pass

def checkDumpToFile():
	removeFiles(['dump1.bin', 'dump2.bin'])

	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]], compactionTest=True, dumpFile = 'dump1.bin')
	o2 = TestObj(a[1], [a[0]], compactionTest=True, dumpFile = 'dump2.bin')
	objs = [o1, o2]
	doTicks(objs, 4.5)

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	o1.addValue(150)
	o2.addValue(200)

	doTicks(objs, 1.5)

	assert o1.getCounter() == 350
	assert o2.getCounter() == 350

	del o1
	del o2

	a = [getNextAddr(), getNextAddr()]
	o1 = TestObj(a[0], [a[1]], compactionTest=1, dumpFile = 'dump1.bin')
	o2 = TestObj(a[1], [a[0]], compactionTest=1, dumpFile = 'dump2.bin')
	objs = [o1, o2]
	doTicks(objs, 4.5)
	assert o1._isReady()
	assert o2._isReady()

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	assert o1.getCounter() == 350
	assert o2.getCounter() == 350

	removeFiles(['dump1.bin', 'dump2.bin'])


def getRandStr():
	return '%0100000x' % random.randrange(16 ** 100000)


def checkBigStorage():
	removeFiles(['dump1.bin', 'dump2.bin'])

	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]], compactionTest2=True, dumpFile = 'dump1.bin')
	o2 = TestObj(a[1], [a[0]], compactionTest2=True, dumpFile = 'dump2.bin')
	objs = [o1, o2]
	doTicks(objs, 4.5)

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	# Store ~50Mb data.
	testRandStr = getRandStr()
	for i in xrange(0, 500):
		o1.addKeyValue(i, getRandStr())
	o1.addKeyValue('test', testRandStr)

	# Wait for replication.
	doTicks(objs, 40.0)

	assert o1.getValue('test') == testRandStr

	o1._forceLogCompaction()
	o2._forceLogCompaction()

	# Wait for disk dump
	doTicks(objs, 5.0)


	a = [getNextAddr(), getNextAddr()]
	o1 = TestObj(a[0], [a[1]], compactionTest=1, dumpFile = 'dump1.bin')
	o2 = TestObj(a[1], [a[0]], compactionTest=1, dumpFile = 'dump2.bin')
	objs = [o1, o2]
	# Wait for disk load, election and replication
	doTicks(objs, 5.5)

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	assert o1.getValue('test') == testRandStr
	assert o2.getValue('test') == testRandStr

	removeFiles(['dump1.bin', 'dump2.bin'])

def encryptionCorrectPassword():
	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]], password='asd')
	o2 = TestObj(a[1], [a[0]], password='asd')
	objs = [o1, o2]
	doTicks(objs, 4.5)

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	o1.addValue(150)
	o2.addValue(200)

	doTicks(objs, 1.5)

	assert o1.getCounter() == 350
	assert o2.getCounter() == 350

def encryptionWrongPassword():
	random.seed(12)

	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]], password='asd')
	o2 = TestObj(a[1], [a[2], a[0]], password='asd')
	o3 = TestObj(a[2], [a[0], a[1]], password='qwe')
	objs = [o1, o2, o3]

	doTicks(objs, 4.5)

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()
	assert o3._getLeader() is None

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

def randomTest1():

	random.seed(12)

	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]], randTest=True)
	o2 = TestObj(a[1], [a[2], a[0]], randTest=True)
	o3 = TestObj(a[2], [a[0], a[1]], randTest=True)
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

# Ensure that raftLog after serialization is the same as in serialized data
def logCompactionRegressionTest1():
	random.seed(42)

	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]])
	o2 = TestObj(a[1], [a[0]])
	objs = [o1, o2]

	doTicks(objs, 4.5)

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	o1._forceLogCompaction()
	doTicks(objs, 0.5)
	assert o1._SyncObj__forceLogCompaction == False
	logAfterCompaction = o1._SyncObj__raftLog
	o1._SyncObj__loadDumpFile()
	logAfterDeserialize = o1._SyncObj__raftLog
	assert logAfterCompaction == logAfterDeserialize

def runTests():
	useCrypto = True
	if len(sys.argv) > 1 and sys.argv[1] == 'nocrypto':
		useCrypto = False

	syncTwoObjects()
	syncThreeObjectsLeaderFail()
	manyActionsLogCompaction()
	logCompactionRegressionTest1()
	checkCallbacksSimple()
	checkDumpToFile()
	checkBigStorage()
	randomTest1()
	if useCrypto:
		encryptionCorrectPassword()
		encryptionWrongPassword()
	print '[SUCCESS]'

if __name__ == '__main__':
	runTests()

