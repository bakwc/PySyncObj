import time
import socket
import zlib
import cPickle
import struct


class Connection(object):
    def __init__(self, socket=None, timeout=10.0):
        self.__socket = socket
        self.__readBuffer = ''
        self.__writeBuffer = ''
        self.__lastReadTime = time.time()
        self.__timeout = timeout
        self.__disconnected = socket is None
        self.__fileno = None if socket is None else socket.fileno()

    def __del__(self):
        self.__socket = None

    def isDisconnected(self):
        return self.__disconnected or time.time() - self.__lastReadTime > self.__timeout

    def connect(self, host, port):
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2 ** 13)
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2 ** 13)
        self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.__socket.setblocking(0)
        self.__readBuffer = ''
        self.__writeBuffer = ''
        self.__lastReadTime = time.time()

        try:
            self.__socket.connect((host, port))
        except socket.error as e:
            if e.errno != socket.errno.EINPROGRESS:
                return False
        self.__fileno = self.__socket.fileno()
        self.__disconnected = False
        return True

    def send(self, message):
        data = zlib.compress(cPickle.dumps(message, -1), 3)
        data = struct.pack('i', len(data)) + data
        self.__writeBuffer += data
        self.trySendBuffer()

    def trySendBuffer(self):
        while self.processSend():
            pass

    def processSend(self):
        if not self.__writeBuffer:
            return False
        try:
            res = self.__socket.send(self.__writeBuffer)
            if res < 0:
                self.__writeBuffer = ''
                self.__readBuffer = ''
                self.__disconnected = True
                return False
            if res == 0:
                return False

            self.__writeBuffer = self.__writeBuffer[res:]
            return True
        except socket.error as e:
            if e.errno != socket.errno.EAGAIN:
                self.__writeBuffer = ''
                self.__readBuffer = ''
                self.__disconnected = True
            return False

    def getSendBufferSize(self):
        return len(self.__writeBuffer)

    def read(self):
        try:
            incoming = self.__socket.recv(2 ** 13)
        except socket.error as e:
            if e.errno != socket.errno.EAGAIN:
                self.__disconnected = True
            return False
        if self.__socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
            self.__disconnected = True
            return False
        if not incoming:
            return False
        self.__lastReadTime = time.time()
        self.__readBuffer += incoming
        return True

    def getMessage(self):
        if len(self.__readBuffer) < 4:
            return None
        l = struct.unpack('i', self.__readBuffer[:4])[0]
        if len(self.__readBuffer) - 4 < l:
            return None
        data = self.__readBuffer[4:4 + l]
        try:
            message = cPickle.loads(zlib.decompress(data))
        except (zlib.error, cPickle.UnpicklingError):
            self.__disconnected = True
            return None
        self.__readBuffer = self.__readBuffer[4 + l:]
        return message

    def close(self):
        self.__socket.close()
        self.__disconnected = True

    def socket(self):
        return self.__socket

    def fileno(self):
        return self.__fileno
