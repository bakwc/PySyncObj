import time
import socket
import zlib
import struct

import pysyncobj.pickle as pickle
import pysyncobj.win_inet_pton

from .poller import POLL_EVENT_TYPE
from .monotonic import monotonic as monotonicTime


class CONNECTION_STATE:
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2

def _getAddrType(addr):
    try:
        socket.inet_aton(addr)
        return socket.AF_INET
    except socket.error:
        pass
    try:
        socket.inet_pton(socket.AF_INET6, addr)
        return socket.AF_INET6
    except socket.error:
        pass
    raise Exception('unknown address type')

class TcpConnection(object):

    def __init__(self, poller, onMessageReceived = None, onConnected = None, onDisconnected = None,
                 socket=None, timeout=10.0, sendBufferSize = 2 ** 13, recvBufferSize = 2 ** 13):

        self.sendRandKey = None
        self.recvRandKey = None
        self.encryptor = None

        self.__socket = socket
        self.__readBuffer = bytes()
        self.__writeBuffer = bytes()
        self.__lastReadTime = monotonicTime()
        self.__timeout = timeout
        self.__poller = poller
        if socket is not None:
            self.__socket = socket
            self.__fileno = socket.fileno()
            self.__state = CONNECTION_STATE.CONNECTED
            self.__poller.subscribe(self.__fileno,
                                     self.__processConnection,
                                     POLL_EVENT_TYPE.READ | POLL_EVENT_TYPE.WRITE | POLL_EVENT_TYPE.ERROR)
        else:
            self.__state = CONNECTION_STATE.DISCONNECTED
            self.__fileno = None
            self.__socket = None

        self.__onMessageReceived = onMessageReceived
        self.__onConnected = onConnected
        self.__onDisconnected = onDisconnected
        self.__sendBufferSize = sendBufferSize
        self.__recvBufferSize = recvBufferSize

    def setOnConnectedCallback(self, onConnected):
        self.__onConnected = onConnected

    def setOnMessageReceivedCallback(self, onMessageReceived):
        self.__onMessageReceived = onMessageReceived

    def setOnDisconnectedCallback(self, onDisconnected):
        self.__onDisconnected = onDisconnected

    def connect(self, host, port):
        if host is None:
            return False
        self.__state = CONNECTION_STATE.DISCONNECTED
        self.__fileno = None
        self.__socket = socket.socket(_getAddrType(host), socket.SOCK_STREAM)
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.__sendBufferSize)
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.__recvBufferSize)
        self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.__socket.setblocking(0)
        self.__readBuffer = bytes()
        self.__writeBuffer = bytes()
        self.__lastReadTime = monotonicTime()

        try:
            self.__socket.connect((host, port))
        except socket.error as e:
            if e.errno not in (socket.errno.EINPROGRESS, socket.errno.EWOULDBLOCK):
                return False
        self.__fileno = self.__socket.fileno()
        self.__state = CONNECTION_STATE.CONNECTING
        self.__poller.subscribe(self.__fileno,
                                 self.__processConnection,
                                 POLL_EVENT_TYPE.READ | POLL_EVENT_TYPE.WRITE | POLL_EVENT_TYPE.ERROR)
        return True

    def send(self, message):
        if self.sendRandKey:
            message = (self.sendRandKey, message)
        data = zlib.compress(pickle.dumps(message), 3)
        if self.encryptor:
            data = self.encryptor.encrypt(data)
        data = struct.pack('i', len(data)) + data
        self.__writeBuffer += data
        self.__trySendBuffer()

    def fileno(self):
        return self.__fileno

    def disconnect(self):
        needCallDisconnect = False
        if self.__onDisconnected is not None and self.__state != CONNECTION_STATE.DISCONNECTED:
            needCallDisconnect = True
        self.sendRandKey = None
        self.recvRandKey = None
        if self.__socket is not None:
            self.__socket.close()
            self.__socket = None
        if self.__fileno is not None:
            self.__poller.unsubscribe(self.__fileno)
            self.__fileno = None
        self.__writeBuffer = bytes()
        self.__readBuffer = bytes()
        self.__state = CONNECTION_STATE.DISCONNECTED
        if needCallDisconnect:
            self.__onDisconnected()

    def getSendBufferSize(self):
        return len(self.__writeBuffer)

    def __processConnection(self, descr, eventType):
        poller = self.__poller
        if descr != self.__fileno:
            poller.unsubscribe(descr)
            return

        if eventType & POLL_EVENT_TYPE.ERROR:
            self.disconnect()
            return

        if monotonicTime() - self.__lastReadTime > self.__timeout:
            self.disconnect()
            return

        if eventType & POLL_EVENT_TYPE.READ or eventType & POLL_EVENT_TYPE.WRITE:
            if self.__socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
                self.disconnect()
                return

            if self.__state == CONNECTION_STATE.CONNECTING:
                if self.__onConnected is not None:
                    self.__onConnected()
                self.__state = CONNECTION_STATE.CONNECTED
                self.__lastReadTime = monotonicTime()
                return

        if eventType & POLL_EVENT_TYPE.WRITE:
            self.__trySendBuffer()
            if self.__state == CONNECTION_STATE.DISCONNECTED:
                return
            event = POLL_EVENT_TYPE.READ | POLL_EVENT_TYPE.ERROR
            if len(self.__writeBuffer) > 0:
                event |= POLL_EVENT_TYPE.WRITE
            poller.subscribe(descr, self.__processConnection, event)

        if eventType & POLL_EVENT_TYPE.READ:
            self.__tryReadBuffer()
            if self.__state == CONNECTION_STATE.DISCONNECTED:
                return

            while True:
                message = self.__processParseMessage()
                if message is None:
                    break
                if self.__onMessageReceived is not None:
                    self.__onMessageReceived(message)
                if self.__state == CONNECTION_STATE.DISCONNECTED:
                    return

    def __trySendBuffer(self):
        while self.__processSend():
            pass

    def __processSend(self):
        if not self.__writeBuffer:
            return False
        try:
            res = self.__socket.send(self.__writeBuffer)
            if res < 0:
                self.disconnect()
                return False
            if res == 0:
                return False
            self.__writeBuffer = self.__writeBuffer[res:]
            return True
        except socket.error as e:
            if e.errno not in (socket.errno.EAGAIN, socket.errno.EWOULDBLOCK):
                self.disconnect()
            return False

    def __tryReadBuffer(self):
        while self.__processRead():
            pass
        self.__lastReadTime = monotonicTime()

    def __processRead(self):
        try:
            incoming = self.__socket.recv(self.__recvBufferSize)
        except socket.error as e:
            if e.errno not in (socket.errno.EAGAIN, socket.errno.EWOULDBLOCK):
                self.disconnect()
            return False
        if self.__socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
            self.disconnect()
            return False
        if not incoming:
            self.disconnect()
            return False
        self.__readBuffer += incoming
        return True

    def __processParseMessage(self):
        if len(self.__readBuffer) < 4:
            return None
        l = struct.unpack('i', self.__readBuffer[:4])[0]
        if len(self.__readBuffer) - 4 < l:
            return None
        data = self.__readBuffer[4:4 + l]
        try:
            if self.encryptor:
                data = self.encryptor.decrypt(data)
            message = pickle.loads(zlib.decompress(data))
            if self.recvRandKey:
                randKey, message = message
                assert randKey == self.recvRandKey
        except:
            self.disconnect()
            return None
        self.__readBuffer = self.__readBuffer[4 + l:]
        return message

    @property
    def state(self):
        return self.__state
