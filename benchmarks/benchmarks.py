import sys
from subprocess import Popen, PIPE, STDOUT
import os
DEVNULL = open(os.devnull, 'wb')

START_PORT = 4321
MIN_RPS = 10
MAX_RPS = 40000

def singleBenchmark(requestsPerSecond, requestSize, numNodes):
    rpsPerNode = requestsPerSecond / numNodes
    cmd = 'python2.7 testobj.py %d %d' % (rpsPerNode, requestSize)
    processes = []
    allAddrs = []
    for i in xrange(numNodes):
        allAddrs.append('localhost:%d' % (START_PORT + i))
    for i in xrange(numNodes):
        addrs = list(allAddrs)
        selfAddr = addrs.pop(i)
        addrs = [selfAddr] + addrs
        currCmd = cmd + ' ' + ' '.join(addrs)
        p = Popen(currCmd, shell=True, stdin=PIPE, stdout=DEVNULL, stderr=STDOUT)
        processes.append(p)
    for p in processes:
        p.communicate()
        if p.returncode != 0:
            return False
    return True

def detectMaxRps(requestSize, numNodes):
    a = MIN_RPS
    b = MAX_RPS
    while b - a > MIN_RPS:
        c = a + (b - a) / 2
        res = singleBenchmark(c, requestSize, numNodes)
        if res:
            a = c
        else:
            b = c
        print ' CURRENT MAX RPS:', a
    return a

if __name__ == '__main__':
    #print detectMaxRps(10, 3)
    print detectMaxRps(10, 6)
    #res = singleBenchmark(300, 10, 3)
    #print ' === RESULT:', res

    # if len(sys.argv) < 5:
    #     print 'Usage: %s RPS requestSize ' % sys.argv[0]
    #     sys.exit(-1)
