#
#  WARNING: this is generated file, use generate.sh to update it.
#
import os
import zlib
import pickle
import gzip
from .debug_utils import LOG_WARNING, LOG_CURRENT_EXCEPTION
from .atomic_replace import atomicReplace
from .config import SERIALIZER_STATE


class Serializer(object):
    def __init__(self, fileName, transmissionBatchSize, tryUseFork,
                 serializer, deserializer, serializeChecker):
        self.__useFork = tryUseFork and hasattr(os, 'fork') and serializer is None
        self.__fileName = fileName
        self.__transmissionBatchSize = transmissionBatchSize
        self.__pid = 0
        self.__currentID = 0
        self.__transmissions = {}
        self.__incomingTransmissionFile = None
        self.__inMemorySerializedData = None
        self.__serializer = serializer
        self.__deserializer = deserializer
        self.__serializeChecker = serializeChecker

    def checkSerializing(self):
        if self.__serializeChecker is not None:
            status = self.__serializeChecker()
            if status in (SERIALIZER_STATE.SUCCESS, SERIALIZER_STATE.FAILED):
                self.__pid = 0
            return status, self.__currentID

        # In-memory case
        if self.__fileName is None or not self.__useFork:
            if self.__pid in (-1, -2):
                serializeState = SERIALIZER_STATE.SUCCESS if self.__pid == -1 else SERIALIZER_STATE.FAILED
                self.__pid = 0
                self.__transmissions = {}
                return serializeState, self.__currentID
            return SERIALIZER_STATE.NOT_SERIALIZING, None

        # File case
        pid = self.__pid
        if pid == 0:
            return SERIALIZER_STATE.NOT_SERIALIZING, None
        try:
            rpid, status = os.waitpid(pid, os.WNOHANG)
        except OSError:
            self.__pid = 0
            return SERIALIZER_STATE.FAILED, self.__currentID
        if rpid == pid:
            if status == 0:
                self.__transmissions = {}
                self.__pid = 0
                return SERIALIZER_STATE.SUCCESS, self.__currentID
            self.__pid = 0
            return SERIALIZER_STATE.FAILED, self.__currentID
        return SERIALIZER_STATE.SERIALIZING, self.__currentID

    def serialize(self, data, id):
        if self.__pid != 0:
            return

        self.__currentID = id

        # In-memory case
        if self.__fileName is None:
            self.__inMemorySerializedData = zlib.compress(pickle.dumps(data, -1))
            self.__pid = -1
            return

        # File case
        if self.__useFork:
            pid = os.fork()
            if pid != 0:
                self.__pid = pid
                return

        try:
            tmpFile = self.__fileName + '.tmp'
            if self.__serializer is not None:
                self.__serializer(tmpFile, data[1:])
            else:
                with open(tmpFile, 'wb') as f:
                    with gzip.GzipFile(fileobj=f) as g:
                        pickle.dump(data, g, -1)

            atomicReplace(tmpFile, self.__fileName)
            if self.__useFork:
                os._exit(0)
            else:
                self.__pid = -1
        except Exception as e:
            if self.__useFork:
                os._exit(-1)
            else:
                self.__pid = -2

    def deserialize(self):
        if self.__fileName is None:
            return pickle.loads(zlib.decompress(self.__inMemorySerializedData))

        if self.__deserializer is not None:
            return (None,) + self.__deserializer(self.__fileName)
        else:
            with open(self.__fileName, 'rb') as f:
                with gzip.GzipFile(fileobj=f) as g:
                    return pickle.load(g)

    def getTransmissionData(self, transmissionID):
        if self.__pid != 0:
            return None
        transmission = self.__transmissions.get(transmissionID, None)
        if transmission is None:
            try:
                if self.__fileName is None:
                    data = self.__inMemorySerializedData
                    assert data is not None
                    self.__transmissions[transmissionID] = transmission = {
                        'transmitted': 0,
                        'data': data,
                    }
                else:
                    self.__transmissions[transmissionID] = transmission = {
                        'file': open(self.__fileName, 'rb'),
                        'transmitted': 0,
                    }
            except:
                LOG_CURRENT_EXCEPTION()
                LOG_WARNING('Failed to open file for transmission')
                self.__transmissions.pop(transmissionID, None)
                return None
        isFirst = transmission['transmitted'] == 0
        try:
            if self.__fileName is None:
                transmitted = transmission['transmitted']
                data = transmission['data'][transmitted:transmitted + self.__transmissionBatchSize]
            else:
                data = transmission['file'].read(self.__transmissionBatchSize)
        except:
            LOG_WARNING('Error reading transmission file')
            self.__transmissions.pop(transmissionID, None)
            return False
        size = len(data)
        transmission['transmitted'] += size
        isLast = size == 0
        if isLast:
            self.__transmissions.pop(transmissionID, None)
        return data, isFirst, isLast

    def setTransmissionData(self, data):
        if data is None:
            return False
        data, isFirst, isLast = data

        # In-memory case
        if self.__fileName is None:
            if isFirst:
                self.__incomingTransmissionFile = bytes()
            elif self.__incomingTransmissionFile is None:
                return False
            self.__incomingTransmissionFile += data
            if isLast:
                self.__inMemorySerializedData = self.__incomingTransmissionFile
                self.__incomingTransmissionFile = None
                return True
            return False

        # File case
        tmpFile = self.__fileName + '.1.tmp'
        if isFirst:
            if self.__incomingTransmissionFile is not None:
                self.__incomingTransmissionFile.close()
            try:
                self.__incomingTransmissionFile = open(tmpFile, 'wb')
            except:
                LOG_WARNING('Failed to open file for incoming transition')
                LOG_CURRENT_EXCEPTION()
                self.__incomingTransmissionFile = None
                return False
        elif self.__incomingTransmissionFile is None:
            return False
        try:
            self.__incomingTransmissionFile.write(data)
        except:
            LOG_WARNING('Failed to write incoming transition data')
            LOG_CURRENT_EXCEPTION()
            self.__incomingTransmissionFile = None
            return False
        if isLast:
            self.__incomingTransmissionFile.close()
            self.__incomingTransmissionFile = None
            try:
                atomicReplace(tmpFile, self.__fileName)
            except:
                LOG_WARNING('Failed to rename temporary incoming transition file')
                LOG_CURRENT_EXCEPTION()
                return False
            return True
        return False

    def cancelTransmisstion(self, id):
        self.__transmissions.pop(id, None)
