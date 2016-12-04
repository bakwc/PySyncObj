from __future__ import print_function
import sys
import time
import random
from collections import defaultdict
sys.path.append("../")
from pysyncobj import SyncObj, replicated, SyncObjConf, FAIL_REASON

class TestObj(SyncObj):

    def __init__(self, selfNodeAddr, otherNodeAddrs):
        cfg = SyncObjConf(
            appendEntriesUseBatch=False,
        )
        super(TestObj, self).__init__(selfNodeAddr, otherNodeAddrs, cfg)
        self.__appliedCommands = 0

    @replicated
    def testMethod(self, val, callTime):
        self.__appliedCommands += 1
        return (callTime, time.time())

    def getNumCommandsApplied(self):
        return self.__appliedCommands

_g_sent = 0
_g_success = 0
_g_error = 0
_g_errors = defaultdict(int)
_g_delays = []

def clbck(res, err):
    global _g_error, _g_success, _g_delays
    if err == FAIL_REASON.SUCCESS:
        _g_success += 1
        callTime, recvTime = res
        delay = time.time() - callTime
        _g_delays.append(delay)
    else:
        _g_error += 1
        _g_errors[err] += 1

def getRandStr(l):
    f = '%0' + str(l) + 'x'
    return f % random.randrange(16 ** l)

if __name__ == '__main__':
    if len(sys.argv) < 5:
        print('Usage: %s RPS requestSize selfHost:port partner1Host:port partner2Host:port ...' % sys.argv[0])
        sys.exit(-1)

    numCommands = int(sys.argv[1])
    cmdSize = int(sys.argv[2])

    selfAddr = sys.argv[3]
    if selfAddr == 'readonly':
        selfAddr = None
    partners = sys.argv[4:]

    maxCommandsQueueSize = int(0.9 * SyncObjConf().commandsQueueSize / len(partners))

    obj = TestObj(selfAddr, partners)

    while obj._getLeader() is None:
        time.sleep(0.5)

    time.sleep(4.0)

    startTime = time.time()

    while time.time() - startTime < 25.0:
        st = time.time()
        for i in xrange(0, numCommands):
            obj.testMethod(getRandStr(cmdSize), time.time(), callback=clbck)
            _g_sent += 1
        delta = time.time() - st
        assert delta <= 1.0
        time.sleep(1.0 - delta)

    time.sleep(4.0)

    successRate = float(_g_success) / float(_g_sent)
    print('SUCCESS RATE:', successRate)

    delays = sorted(_g_delays)
    avgDelay = _g_delays[len(_g_delays) / 2]
    print('AVG DELAY:', avgDelay)

    if successRate < 0.9:
        print('LOST RATE:', 1.0 - float(_g_success + _g_error) / float(_g_sent))
        print('ERRORS STATS: %d' % len(_g_errors))
        for err in _g_errors:
            print(err, float(_g_errors[err]) / float(_g_error))

    sys.exit(int(avgDelay * 100))
