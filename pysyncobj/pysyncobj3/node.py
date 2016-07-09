#
#  WARNING: this is generated file, use gen_py3.sh to update it.
#
import weakref
import time
import os
from .tcp_connection import TcpConnection
from .dns_resolver import globalDnsResolver
from .poller import globalPoller

class NODE_STATUS:
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2


class Node(object):
    def __init__(self, syncObj, nodeAddr):
        self.__syncObj = weakref.ref(syncObj)
        self.__nodeAddr = nodeAddr
        self.__ip = globalDnsResolver().resolve(nodeAddr.split(':')[0])
        self.__port = int(nodeAddr.split(':')[1])
        self.__poller = globalPoller()
        self.__shouldConnect = syncObj._getSelfNodeAddr() > nodeAddr
        self.__encryptor = syncObj._getEncryptor()
        self.__conn = None
        if self.__shouldConnect:
            self.__conn = TcpConnection(poller=syncObj._poller,
                                        onConnected=self.__onConnected,
                                        onMessageReceived=self.__onMessageReceived,
                                        onDisconnected=self.__onDisconnected,
                                        timeout=syncObj._getConf().connectionTimeout,
                                        sendBufferSize=syncObj._getConf().sendBufferSize,
                                        recvBufferSize=syncObj._getConf().recvBufferSize)
            self.__conn.encryptor = self.__encryptor

        self.__lastConnectAttemptTime = 0
        self.__lastPingTime = 0
        self.__status = NODE_STATUS.DISCONNECTED

    def __del__(self):
        self.__conn = None
        self.__poller = None

    def __onConnected(self):
        self.__status = NODE_STATUS.CONNECTED
        if self.__encryptor:
            self.__conn.recvRandKey = os.urandom(32)
            self.__conn.send(self.__conn.recvRandKey)
            return
        self.__conn.send(self.__syncObj()._getSelfNodeAddr())

    def __onDisconnected(self):
        self.__status = NODE_STATUS.DISCONNECTED

    def __onMessageReceived(self, message):
        if self.__encryptor and not self.__conn.sendRandKey:
            self.__conn.sendRandKey = message
            self.__conn.send(self.__syncObj()._getSelfNodeAddr())
            return

        self.__syncObj()._onMessageReceived(self.__nodeAddr, message)

    def onPartnerConnected(self, conn):
        self.__conn = conn
        conn.setOnMessageReceivedCallback(self.__onMessageReceived)
        conn.setOnDisconnectedCallback(self.__onDisconnected)
        self.__status = NODE_STATUS.CONNECTED

    def getStatus(self):
        return self.__status

    def isConnected(self):
        return self.__status == NODE_STATUS.CONNECTED

    def getAddress(self):
        return self.__nodeAddr

    def getSendBufferSize(self):
        return self.__conn.getSendBufferSize()

    def send(self, message):
        if self.__status != NODE_STATUS.CONNECTED:
            return False
        self.__conn.send(message)
        if self.__status != NODE_STATUS.CONNECTED:
            return False
        return True

    def connectIfRequired(self):
        if not self.__shouldConnect:
            return
        if self.__status != NODE_STATUS.DISCONNECTED:
            return
        if time.time() - self.__lastConnectAttemptTime < self.__syncObj()._getConf().connectionRetryTime:
            return
        self.__status = NODE_STATUS.CONNECTING
        self.__lastConnectAttemptTime = time.time()
        if not self.__conn.connect(self.__ip, self.__port):
            self.__status = NODE_STATUS.DISCONNECTED
            return
