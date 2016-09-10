#!/usr/bin/env python2

import sys
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
sys.path.append("../")
from pysyncobj import SyncObj, replicated


class KVStorage(SyncObj):
    def __init__(self, selfAddress, partnerAddrs):
        super(KVStorage, self).__init__(selfAddress, partnerAddrs)
        self.__data = {}

    @replicated
    def set(self, key, value):
        self.__data[key] = value

    @replicated
    def pop(self, key):
        self.__data.pop(key, None)

    def get(self, key):
        return self.__data.get(key, None)

_g_kvstorage = None


def main():
    if len(sys.argv) < 2:
        print 'Usage: %s selfHost:port partner1Host:port partner2Host:port ...'
        sys.exit(-1)

    selfAddr = sys.argv[1]
    if selfAddr == 'readonly':
        selfAddr = None
    partners = []
    for i in xrange(2, len(sys.argv)):
        partners.append(sys.argv[i])

    global _g_kvstorage
    _g_kvstorage = KVStorage(selfAddr, partners)

    while True:
        cmd = raw_input(">> ").split()
        if not cmd:
            continue
        elif cmd[0] == 'set':
            _g_kvstorage.set(cmd[1], cmd[2])
        elif cmd[0] == 'get':
            print _g_kvstorage.get(cmd[1])
        elif cmd[0] == 'pop':
            print _g_kvstorage.pop(cmd[1])
        else:
            print 'Wrong command'

if __name__ == '__main__':
    main()
