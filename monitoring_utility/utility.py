from pysyncobj.encryptor import getEncryptor
from pysyncobj.poller import createPoller
from pysyncobj.tcp_connection import TcpConnection
from parser import Parser
import os


class Utility(object):

    def __init__(self, args):

        if self.__getData(args):
            self.__result = 'None'
            self.__poller = createPoller('auto')
            self.__connection = TcpConnection(self.__poller, onDisconnected=self.__onDisconnected, onMessageReceived=self.__onMessageReceived, onConnected=self.__onConnected)
            if self.__password is not None:
                self.__connection.encryptor = getEncryptor(self.__password)
            self.__isConnected = self.__connection.connect(self.__host, self.__port)
            while self.__isConnected:
                self.__poller.poll(0.5)


    def __onMessageReceived(self, message):

        if self.__connection.encryptor and not self.__connection.sendRandKey:
            self.__connection.sendRandKey = message
            self.__connection.send(self.__data)
            return
        self.__result = message
        self.__connection.disconnect()

    def __onDisconnected(self):
        self.__isConnected = False

    def __onConnected(self):

        if self.__connection.encryptor:
            self.__connection.recvRandKey = os.urandom(32)
            self.__connection.send(self.__connection.recvRandKey)
            return
        self.__connection.send(self.__data)

    def getResult(self):
        return self.__result

    def __getData(self, args):

        parser = Parser()
        data = parser.parse(args)
        if not self.__checkCorrectAdress(data.connection):
            self.__result = 'invalid adress to connect'
            return False
        self.__host, self.__port = data.connection.split(':')
        self.__port = int(self.__port)

        self.__password = data.password
        if data.status and data.add is None and data.remove is None:
            self.__data = 'status'
            return True
        elif data.add is not None and data.remove is None and not data.status:
            if not self.__checkCorrectAdress(data.add):
                self.__result = 'invalid adress to command add'
                return False
            self.__data = 'add' + data.add
            return True
        elif data.remove is not None and data.add is None and not data.status:
            if not self.__checkCorrectAdress(data.remove):
                self.__result = 'invalid adress to command remove'
                return False
            self.__data = 'remove' + data.remove
            return True
        else:
            self.__result = 'invalid command'
            return False


    def __checkCorrectAdress(self, adress):

        try:
            host, port = adress.split(':')
            port = int(port)
            assert (port > 0 and port < 65536)
            return True
        except:
            return False
