import sys
import os, pickle
from functools import wraps
from subprocess import Popen, PIPE, STDOUT
import os
DEVNULL = open(os.devnull, 'wb')

START_PORT = 4321
MIN_RPS = 10
MAX_RPS = 40000

def memoize(fileName):
    def doMemoize(func):
        if os.path.exists(fileName):
            with open(fileName) as f:
                cache = pickle.load(f)
        else:
            cache = {}
        @wraps(func)
        def wrap(*args):
            if args not in cache:
                cache[args] = func(*args)
                with open(fileName, 'wb') as f:
                    pickle.dump(cache, f)
            return cache[args]
        return wrap
    return doMemoize

def singleBenchmark(requestsPerSecond, requestSize, numNodes, numNodesReadonly = 0):
    rpsPerNode = requestsPerSecond / (numNodes + numNodesReadonly)
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
        p = Popen(currCmd, shell=True, stdin=PIPE)
        processes.append(p)
    for i in xrange(numNodesReadonly):
        addrs = list(allAddrs)
        addrs = ['readonly'] + addrs
        currCmd = cmd + ' ' + ' '.join(addrs)
        p = Popen(currCmd, shell=True, stdin=PIPE)
        processes.append(p)
    errRates = []
    for p in processes:
        p.communicate()
        errRates.append(float(p.returncode) / 100.0)
    avgRate = sum(errRates) / len(errRates)
    #print 'average success rate:', avgRate
    return avgRate >= 0.9

def doDetectMaxRps(requestSize, numNodes):
    a = MIN_RPS
    b = MAX_RPS
    numIt = 0
    while b - a > MIN_RPS:
        c = a + (b - a) / 2
        res = singleBenchmark(c, requestSize, numNodes)
        if res:
            a = c
        else:
            b = c
        print 'subiteration %d, current max %d' % (numIt, a)
        numIt += 1
    return a

@memoize('maxRpsCache.bin')
def detectMaxRps(requestSize, numNodes):
    results = []
    for i in xrange(0, 5):
        res = doDetectMaxRps(requestSize, numNodes)
        print 'iteration %d, current max %d' % (i, res)
        results.append(res)
    return sorted(results)[len(results) / 2]

if __name__ == '__main__':
    for i in xrange(10, 2100, 500):
        res = detectMaxRps(i, 3)
        print 'request size: %d, rps: %d' % (i, int(res))

    for i in xrange(3, 8):
        res = detectMaxRps(200, i)
        print 'nodes number: %d, rps: %d' % (i, int(res))
