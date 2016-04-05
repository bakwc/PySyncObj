import weakref
import time
import socket
from connection import Connection
from poller import POLL_EVENT_TYPE


class NODE_STATUS:
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2


class Node(object):
    def __init__(self, syncObj, nodeAddr):
        self.__syncObj = weakref.ref(syncObj)
        self.__nodeAddr = nodeAddr
        self.__ip = syncObj._getResolver().resolve(nodeAddr.split(':')[0])
        self.__port = int(nodeAddr.split(':')[1])
        self.__poller = syncObj._getPoller()
        self.__conn = Connection(socket=None, timeout=syncObj._getConf().connectionTimeout)

        self.__shouldConnect = syncObj._getSelfNodeAddr() > nodeAddr
        self.__lastConnectAttemptTime = 0
        self.__lastPingTime = 0
        self.__status = NODE_STATUS.DISCONNECTED

    def __del__(self):
        self.__conn = None
        self.__poller = None

    def onPartnerConnected(self, conn):
        self.__conn = conn
        self.__status = NODE_STATUS.CONNECTED
        self.__poller.subscribe(self.__conn.fileno(),
                                self.__processConnection,
                                POLL_EVENT_TYPE.READ | POLL_EVENT_TYPE.WRITE | POLL_EVENT_TYPE.ERROR)

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
        if self.__conn.isDisconnected():
            self.__status = NODE_STATUS.DISCONNECTED
            self.__poller.unsubscribe(self.__conn.fileno())
            self.__conn.close()
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
        self.__poller.subscribe(self.__conn.fileno(),
                                self.__processConnection,
                                POLL_EVENT_TYPE.READ | POLL_EVENT_TYPE.WRITE | POLL_EVENT_TYPE.ERROR)

    def __processConnection(self, descr, eventType):
        if descr != self.__conn.fileno():
            self.__poller.unsubscribe(descr)
            return

        isError = False
        if eventType & POLL_EVENT_TYPE.ERROR:
            isError = True

        if eventType & POLL_EVENT_TYPE.READ or eventType & POLL_EVENT_TYPE.WRITE:
            if self.__conn.socket().getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
                isError = True
            else:
                if self.__status == NODE_STATUS.CONNECTING:
                    self.__conn.send(self.__syncObj()._getSelfNodeAddr())
                    self.__status = NODE_STATUS.CONNECTED

        if isError or self.__conn.isDisconnected():
            self.__status = NODE_STATUS.DISCONNECTED
            self.__conn.close()
            self.__poller.unsubscribe(descr)
            return

        if eventType & POLL_EVENT_TYPE.WRITE:
            if self.__status == NODE_STATUS.CONNECTING:
                self.__conn.send(self.__syncObj()._getSelfNodeAddr())
                self.__status = NODE_STATUS.CONNECTED
            self.__conn.trySendBuffer()
            event = POLL_EVENT_TYPE.READ | POLL_EVENT_TYPE.ERROR
            if self.__conn.getSendBufferSize() > 0:
                event |= POLL_EVENT_TYPE.WRITE
            if not self.__conn.isDisconnected():
                self.__poller.subscribe(descr, self.__processConnection, event)

        if eventType & POLL_EVENT_TYPE.READ:
            if self.__conn.read():
                while True:
                    message = self.__conn.getMessage()
                    if message is None:
                        break
                    self.__syncObj()._onMessageReceived(self.__nodeAddr, message)
