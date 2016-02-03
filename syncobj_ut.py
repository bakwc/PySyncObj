#!/usr/bin/env python

import sys
import time
from functools import partial
from syncobj import SyncObj, replicated

class TestObj(SyncObj):

	def __init__(self, selfNodeAddr, otherNodeAddrs):
		super(TestObj, self).__init__(selfNodeAddr, otherNodeAddrs, autoTick=False)
		self.__counter = 0

	@replicated
	def addValue(self, value):
		self.__counter += value
		return self.__counter

	def getCounter(self):
		return self.__counter

def doTicks(objects, timeToTick, interval = 0.05):
	currTime = time.time()
	finishTime = currTime + timeToTick
	while currTime < finishTime:
		for o in objects:
			o._onTick(0.0)
		time.sleep(interval)
		currTime = time.time()

_g_nextAddress = 6000 + 60 * (int(time.time()) % 600)

def getNextAddr():
	global _g_nextAddress
	_g_nextAddress += 1
	return 'localhost:%d' % _g_nextAddress

def syncTwoObjects():
	a = [getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1]])
	o2 = TestObj(a[1], [a[0]])
	objs = [o1, o2]
	doTicks(objs, 3.5)

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()

	o1.addValue(150)
	o2.addValue(200)

	doTicks(objs, 0.5)

	assert o1.getCounter() == 350
	assert o2.getCounter() == 350

def syncThreeObjectsLeaderFail():
	a = [getNextAddr(), getNextAddr(), getNextAddr()]

	o1 = TestObj(a[0], [a[1], a[2]])
	o2 = TestObj(a[1], [a[2], a[0]])
	o3 = TestObj(a[2], [a[0], a[1]])
	objs = [o1, o2, o3]

	doTicks(objs, 3.5)

	assert o1._getLeader() in a
	assert o1._getLeader() == o2._getLeader()
	assert o1._getLeader() == o3._getLeader()

	o1.addValue(150)
	o2.addValue(200)

	doTicks(objs, 0.5)

	assert o3.getCounter() == 350

	prevLeader = o1._getLeader()

	newObjs = [o for o in objs if o._getSelfNodeAddr() != prevLeader]

	assert len(newObjs) == 2

	doTicks(newObjs, 3.5)
	assert newObjs[0]._getLeader() != prevLeader
	assert newObjs[0]._getLeader() in a
	assert newObjs[0]._getLeader() == newObjs[1]._getLeader()

	newObjs[1].addValue(50)

	doTicks(newObjs, 0.5)

	assert newObjs[0].getCounter() == 400

	doTicks(objs, 0.5)
	for o in objs:
		assert o.getCounter() == 400

def runTests():
	syncTwoObjects()
	syncThreeObjectsLeaderFail()
	print '[SUCCESS]'

if __name__ == '__main__':
	runTests()
