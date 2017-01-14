import time
import socket
import random
import logging


class DnsCachingResolver(object):
    def __init__(self, cacheTime, failCacheTime):
        self.__cache = {}
        self.__cacheTime = cacheTime
        self.__failCacheTime = failCacheTime
        self.__preferedAddrFamily = socket.AF_INET

    def setTimeouts(self, cacheTime, failCacheTime):
        self.__cacheTime = cacheTime
        self.__failCacheTime = failCacheTime

    def resolve(self, hostname):
        currTime = time.time()
        cachedTime, ips = self.__cache.get(hostname, (0, []))
        timePassed = currTime - cachedTime
        if (timePassed > self.__cacheTime) or (not ips and timePassed > self.__failCacheTime):
            prevIps = ips
            ips = self.__doResolve(hostname)
            if not ips:
                ips = prevIps
            self.__cache[hostname] = (currTime, ips)
        return None if not ips else random.choice(ips)

    def setPreferredAddrFamily(self, preferredAddrFamily):
        if preferredAddrFamily is None:
            self.__preferredAddrFamily = None
        elif preferredAddrFamily == 'ipv4':
            self.__preferredAddrFamily = socket.AF_INET
        elif preferredAddrFamily == 'ipv6':
            self.__preferredAddrFamily = socket.AF_INET
        else:
            self.__preferedAddrFamily = preferredAddrFamily

    def __doResolve(self, hostname):
        try:
            addrs = socket.getaddrinfo(hostname, None)
            ips = []
            if self.__preferedAddrFamily is not None:
                ips = list(set([addr[4][0] for addr in addrs\
                                if addr[0] == self.__preferredAddrFamily]))
            if not ips:
                ips = list(set([addr[4][0] for addr in addrs]))
        except socket.gaierror:
            logging.warning('failed to resolve host %s', hostname)
            ips = []
        return ips

_g_resolver = None
def globalDnsResolver():
    global _g_resolver
    if _g_resolver is None:
        _g_resolver = DnsCachingResolver(cacheTime=600.0, failCacheTime=30.0)
    return _g_resolver
