#
#  WARNING: this is generated file, use gen_py3.sh to update it.
#
import time
import random
import os
import pickle
import zlib
import threading
import queue
import weakref
import collections
import functools
from .dns_resolver import globalDnsResolver
from .poller import globalPoller
from .serializer import Serializer, SERIALIZER_STATE
from .tcp_server import TcpServer
from .node import Node
from .config import SyncObjConf, FAIL_REASON
from .debug_utils import LOG_CURRENT_EXCEPTION, LOG_DEBUG, LOG_WARNING
from .encryptor import HAS_CRYPTO, getEncryptor


class _RAFT_STATE:
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


# https://github.com/bakwc/PySyncObj

class SyncObj(object):
    def __init__(self, selfNodeAddr, otherNodesAddrs, conf=None):

        if conf is None:
            self.__conf = SyncObjConf()
        else:
            self.__conf = conf

        if self.__conf.password is not None:
            if not HAS_CRYPTO:
                raise ImportError("Please install 'cryptography' module")
            self.__encryptor = getEncryptor(self.__conf.password)
        else:
            self.__encryptor = None

        self.__selfNodeAddr = selfNodeAddr
        self.__otherNodesAddrs = otherNodesAddrs
        self.__unknownConnections = {}  # descr => _Connection
        self.__raftState = _RAFT_STATE.FOLLOWER
        self.__raftCurrentTerm = 0
        self.__votesCount = 0
        self.__raftLeader = None
        self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
        self.__raftLog = []  # (command, logID, term)
        self.__raftLog.append((None, 1, self.__raftCurrentTerm))
        self.__raftCommitIndex = 1
        self.__raftLastApplied = 1
        self.__raftNextIndex = {}
        self.__raftMatchIndex = {}
        self.__lastSerializedTime = time.time()
        self.__forceLogCompaction = False
        globalDnsResolver().setTimeouts(self.__conf.dnsCacheTime, self.__conf.dnsFailCacheTime)
        self.__serializer = Serializer(self.__conf.fullDumpFile, self.__conf.logCompactionBatchSize)
        self.__isInitialized = False
        self.__lastInitTryTime = 0

        host, port = selfNodeAddr.split(':')
        self.__server = TcpServer(host, port, onNewConnection=self.__onNewConnection,
                                  sendBufferSize=self.__conf.sendBufferSize,
                                  recvBufferSize=self.__conf.recvBufferSize,
                                  connectionTimeout=self.__conf.connectionTimeout)

        self._methodToID = {}
        self._idToMethod = {}
        methods = sorted([m for m in dir(self) if isinstance(getattr(self, m), collections.Callable)])
        for i, method in enumerate(methods):
            self._methodToID[method] = i
            self._idToMethod[i] = getattr(self, method)

        self.__thread = None
        self.__mainThread = None
        self.__initialised = None
        self.__commandsQueue = queue.Queue(self.__conf.commandsQueueSize)
        self.__nodes = []
        self.__newAppendEntriesTime = 0

        self.__commandsWaitingCommit = collections.defaultdict(list)  # logID => [(termID, callback), ...]
        self.__commandsLocalCounter = 0
        self.__commandsWaitingReply = {}  # commandLocalCounter => callback

        self.__properies = set()
        for key in self.__dict__:
            self.__properies.add(key)

        if self.__conf.autoTick:
            self.__mainThread = threading.current_thread()
            self.__initialised = threading.Event()
            self.__thread = threading.Thread(target=SyncObj._autoTickThread, args=(weakref.proxy(self),))
            self.__thread.start()
            while not self.__initialised.is_set():
                pass
        else:
            self.__initInTickThread()

    def __initInTickThread(self):
        try:
            self.__lastInitTryTime = time.time()
            self.__server.bind()
            self.__nodes = []
            for nodeAddr in self.__otherNodesAddrs:
                self.__nodes.append(Node(self, nodeAddr))
                self.__raftNextIndex[nodeAddr] = 0
                self.__raftMatchIndex[nodeAddr] = 0
            self.__needLoadDumpFile = True
            self.__isInitialized = True
        except:
            LOG_CURRENT_EXCEPTION()

    def _applyCommand(self, command, callback):
        try:
            self.__commandsQueue.put_nowait((command, callback))
        except queue.Full:
            callback(None, FAIL_REASON.QUEUE_FULL)

    def _checkCommandsToApply(self):
        for i in range(self.__conf.maxCommandsPerTick):
            if self.__raftLeader is None and self.__conf.commandsWaitLeader:
                break
            try:
                command, callback = self.__commandsQueue.get_nowait()
            except queue.Empty:
                break

            requestNode, requestID = None, None
            if isinstance(callback, tuple):
                requestNode, requestID = callback

            if self.__raftState == _RAFT_STATE.LEADER:
                idx, term = self.__getCurrentLogIndex() + 1, self.__raftCurrentTerm
                self.__raftLog.append((command, idx, term))
                if requestNode is None:
                    if callback is not None:
                        self.__commandsWaitingCommit[idx].append((term, callback))
                else:
                    self.__send(requestNode, {
                        'type': 'apply_command_response',
                        'request_id': requestID,
                        'log_idx': idx,
                        'log_term': term,
                    })
                if not self.__conf.appendEntriesUseBatch:
                    self.__sendAppendEntries()
            elif self.__raftLeader is not None:
                if requestNode is None:
                    message = {
                        'type': 'apply_command',
                        'command': command,
                    }

                    if callback is not None:
                        self.__commandsLocalCounter += 1
                        self.__commandsWaitingReply[self.__commandsLocalCounter] = callback
                        message['request_id'] = self.__commandsLocalCounter

                    self.__send(self.__raftLeader, message)
                else:
                    self.__send(requestNode, {
                        'type': 'apply_command_response',
                        'request_id': requestID,
                        'error': FAIL_REASON.NOT_LEADER,
                    })
            else:
                if requestNode is None:
                    callback(None, FAIL_REASON.MISSING_LEADER)
                else:
                    self.__send(requestNode, {
                        'type': 'apply_command_response',
                        'request_id': requestID,
                        'error': FAIL_REASON.NOT_LEADER,
                    })

    def _autoTickThread(self):
        self.__initInTickThread()
        self.__initialised.set()
        time.sleep(0.1)
        try:
            while True:
                if not self.__mainThread.is_alive():
                    break
                self._onTick(self.__conf.autoTickPeriod)
        except ReferenceError:
            pass

    def _onTick(self, timeToWait=0.0):
        if not self.__isInitialized:
            if time.time() >= self.__lastInitTryTime + self.__conf.bindRetryTime:
                self.__initInTickThread()

        if not self.__isInitialized:
            time.sleep(timeToWait)
            return

        if self.__needLoadDumpFile:
            if self.__conf.fullDumpFile is not None and os.path.isfile(self.__conf.fullDumpFile):
                self.__loadDumpFile()
            self.__needLoadDumpFile = False

        if self.__raftState in (_RAFT_STATE.FOLLOWER, _RAFT_STATE.CANDIDATE):
            if self.__raftElectionDeadline < time.time():
                self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
                self.__raftLeader = None
                self.__raftState = _RAFT_STATE.CANDIDATE
                self.__raftCurrentTerm += 1
                self.__votesCount = 1
                for node in self.__nodes:
                    node.send({
                        'type': 'request_vote',
                        'term': self.__raftCurrentTerm,
                        'last_log_index': self.__getCurrentLogIndex(),
                        'last_log_term': self.__getCurrentLogTerm(),
                    })
                self.__onLeaderChanged()

        if self.__raftState == _RAFT_STATE.LEADER:
            while self.__raftCommitIndex < self.__getCurrentLogIndex():
                nextCommitIndex = self.__raftCommitIndex + 1
                count = 1
                for node in self.__nodes:
                    if self.__raftMatchIndex[node.getAddress()] >= nextCommitIndex:
                        count += 1
                if count > (len(self.__nodes) + 1) / 2:
                    self.__raftCommitIndex = nextCommitIndex
                else:
                    break

            if time.time() > self.__newAppendEntriesTime:
                self.__sendAppendEntries()

        if self.__raftCommitIndex > self.__raftLastApplied:
            count = self.__raftCommitIndex - self.__raftLastApplied
            entries = self.__getEntries(self.__raftLastApplied + 1, count)
            for entry in entries:
                currentTermID = entry[2]
                subscribers = self.__commandsWaitingCommit.pop(entry[1], [])
                res = self.__doApplyCommand(entry[0])
                for subscribeTermID, callback in subscribers:
                    if subscribeTermID == currentTermID:
                        callback(res, FAIL_REASON.SUCCESS)
                    else:
                        callback(None, FAIL_REASON.DISCARDED)

                self.__raftLastApplied += 1

        self._checkCommandsToApply()
        self.__tryLogCompaction()

        for node in self.__nodes:
            node.connectIfRequired()

        globalPoller().poll(timeToWait)


    def _getLastCommitIndex(self):
        return self.__raftCommitIndex

    def _printStatus(self):
        LOG_DEBUG('self', self.__selfNodeAddr)
        LOG_DEBUG('leader', self.__raftLeader)
        LOG_DEBUG('partner nodes', len(self.__nodes))
        for n in self.__nodes:
            LOG_DEBUG(n.getAddress(), n.getStatus())
        LOG_DEBUG('log len:', len(self.__raftLog))
        LOG_DEBUG('log size bytes:', len(zlib.compress(pickle.dumps(self.__raftLog, -1))))
        LOG_DEBUG('last applied:', self.__raftLastApplied)
        LOG_DEBUG('commit idx:', self.__raftCommitIndex)
        LOG_DEBUG('next node idx:', self.__raftNextIndex)

    def _forceLogCompaction(self):
        self.__forceLogCompaction = True

    def __doApplyCommand(self, command):
        args = []
        kwargs = {
            '_doApply': True,
        }
        if not isinstance(command, tuple):
            funcID = command
        elif len(command) == 2:
            funcID, args = command
        else:
            funcID, args, newKwArgs = command
            kwargs.update(newKwArgs)

        return self._idToMethod[funcID](*args, **kwargs)

    def _onMessageReceived(self, nodeAddr, message):

        if message['type'] == 'request_vote':

            if message['term'] > self.__raftCurrentTerm:
                self.__raftCurrentTerm = message['term']
                self.__raftState = _RAFT_STATE.FOLLOWER

            if self.__raftState in (_RAFT_STATE.FOLLOWER, _RAFT_STATE.CANDIDATE):
                lastLogTerm = message['last_log_term']
                lastLogIdx = message['last_log_index']
                if message['term'] >= self.__raftCurrentTerm:
                    if lastLogTerm < self.__getCurrentLogTerm():
                        return
                    if lastLogTerm == self.__getCurrentLogTerm() and \
                            lastLogIdx < self.__getCurrentLogIndex():
                        return

                    self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
                    self.__send(nodeAddr, {
                        'type': 'response_vote',
                        'term': message['term'],
                    })

        if message['type'] == 'append_entries' and message['term'] >= self.__raftCurrentTerm:
            self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
            if self.__raftLeader != nodeAddr:
                self.__onLeaderChanged()
            self.__raftLeader = nodeAddr
            self.__raftCurrentTerm = message['term']
            self.__raftState = _RAFT_STATE.FOLLOWER
            newEntries = message.get('entries', [])
            serialized = message.get('serialized', None)
            leaderCommitIndex = message['commit_index']

            # Regular append entries
            if 'prevLogIdx' in message:
                prevLogIdx = message['prevLogIdx']
                prevLogTerm = message['prevLogTerm']
                prevEntries = self.__getEntries(prevLogIdx)
                if not prevEntries:
                    self.__sendNextNodeIdx(nodeAddr, reset=True)
                    return
                if prevEntries[0][2] != prevLogTerm:
                    self.__deleteEntriesFrom(prevLogIdx)
                    self.__sendNextNodeIdx(nodeAddr, reset=True)
                    return
                if len(prevEntries) > 1:
                    self.__deleteEntriesFrom(prevLogIdx + 1)
                self.__raftLog += newEntries

            # Install snapshot
            elif serialized is not None:
                if self.__serializer.setTransmissionData(serialized):
                    self.__loadDumpFile()

            self.__sendNextNodeIdx(nodeAddr)

            self.__raftCommitIndex = min(leaderCommitIndex, self.__getCurrentLogIndex())

        if message['type'] == 'apply_command':
            if 'request_id' in message:
                self._applyCommand(message['command'], (nodeAddr, message['request_id']))
            else:
                self._applyCommand(message['command'], None)

        if message['type'] == 'apply_command_response':
            requestID = message['request_id']
            error = message.get('error', None)
            callback = self.__commandsWaitingReply.pop(requestID, None)
            if callback is not None:
                if error is not None:
                    callback(None, error)
                else:
                    idx = message['log_idx']
                    term = message['log_term']
                    assert idx > self.__raftLastApplied
                    self.__commandsWaitingCommit[idx].append((term, callback))

        if self.__raftState == _RAFT_STATE.CANDIDATE:
            if message['type'] == 'response_vote' and message['term'] == self.__raftCurrentTerm:
                self.__votesCount += 1

                if self.__votesCount > (len(self.__nodes) + 1) / 2:
                    self.__onBecomeLeader()

        if self.__raftState == _RAFT_STATE.LEADER:
            if message['type'] == 'next_node_idx':
                reset = message['reset']
                nextNodeIdx = message['next_node_idx']

                currentNodeIdx = nextNodeIdx - 1
                if reset:
                    self.__raftNextIndex[nodeAddr] = nextNodeIdx
                self.__raftMatchIndex[nodeAddr] = currentNodeIdx

    def __sendNextNodeIdx(self, nodeAddr, reset=False):
        self.__send(nodeAddr, {
            'type': 'next_node_idx',
            'next_node_idx': self.__getCurrentLogIndex() + 1,
            'reset': reset,
        })

    def __generateRaftTimeout(self):
        minTimeout = self.__conf.raftMinTimeout
        maxTimeout = self.__conf.raftMaxTimeout
        return minTimeout + (maxTimeout - minTimeout) * random.random()

    def __onNewConnection(self, conn):
        descr = conn.fileno()
        self.__unknownConnections[descr] = conn
        if self.__encryptor:
            conn.encryptor = self.__encryptor

        conn.setOnMessageReceivedCallback(functools.partial(self.__onMessageReceived, conn))
        conn.setOnDisconnectedCallback(functools.partial(self.__onDisconnected, conn))

    def __onMessageReceived(self, conn, message):
        if self.__encryptor and not conn.sendRandKey:
            conn.sendRandKey = message
            conn.recvRandKey = os.urandom(32)
            conn.send(conn.recvRandKey)
            return

        descr = conn.fileno()
        partnerNode = None
        for node in self.__nodes:
            if node.getAddress() == message:
                partnerNode = node
                break
        if partnerNode is None:
            conn.disconnect()
            return
        partnerNode.onPartnerConnected(conn)
        self.__unknownConnections.pop(descr, None)

    def __onDisconnected(self, conn):
        self.__unknownConnections.pop(conn.fileno(), None)

    def __getCurrentLogIndex(self):
        return self.__raftLog[-1][1]

    def __getCurrentLogTerm(self):
        return self.__raftLog[-1][2]

    def __getPrevLogIndexTerm(self, nextNodeIndex):
        prevIndex = nextNodeIndex - 1
        entries = self.__getEntries(prevIndex, 1)
        if entries:
            return prevIndex, entries[0][2]
        return None, None

    def __getEntries(self, fromIDx, count=None):
        firstEntryIDx = self.__raftLog[0][1]
        if fromIDx is None or fromIDx < firstEntryIDx:
            return []
        diff = fromIDx - firstEntryIDx
        if count is None:
            return self.__raftLog[diff:]
        return self.__raftLog[diff:diff + count]

    def _isLeader(self):
        return self.__raftState == _RAFT_STATE.LEADER

    def _getLeader(self):
        return self.__raftLeader

    def _getRaftLogSize(self):
        return len(self.__raftLog)

    def __deleteEntriesFrom(self, fromIDx):
        firstEntryIDx = self.__raftLog[0][1]
        diff = fromIDx - firstEntryIDx
        if diff < 0:
            return
        self.__raftLog = self.__raftLog[:diff]

    def __deleteEntriesTo(self, toIDx):
        firstEntryIDx = self.__raftLog[0][1]
        diff = toIDx - firstEntryIDx
        if diff < 0:
            return
        self.__raftLog = self.__raftLog[diff:]

    def __onBecomeLeader(self):
        self.__raftLeader = self.__selfNodeAddr
        self.__raftState = _RAFT_STATE.LEADER

        for node in self.__nodes:
            nodeAddr = node.getAddress()
            self.__raftNextIndex[nodeAddr] = self.__getCurrentLogIndex() + 1
            self.__raftMatchIndex[nodeAddr] = 0

        self.__sendAppendEntries()

    def __onLeaderChanged(self):
        for id in sorted(self.__commandsWaitingReply):
            self.__commandsWaitingReply[id](None, FAIL_REASON.LEADER_CHANGED)
        self.__commandsWaitingReply = {}

    def __sendAppendEntries(self):
        self.__newAppendEntriesTime = time.time() + self.__conf.appendEntriesPeriod

        startTime = time.time()

        for node in self.__nodes:
            nodeAddr = node.getAddress()

            if not node.isConnected():
                self.__serializer.cancelTransmisstion(nodeAddr)
                continue

            sendSingle = True
            sendingSerialized = False
            nextNodeIndex = self.__raftNextIndex[nodeAddr]

            while nextNodeIndex <= self.__getCurrentLogIndex() or sendSingle or sendingSerialized:
                if nextNodeIndex >= self.__raftLog[0][1]:
                    prevLogIdx, prevLogTerm = self.__getPrevLogIndexTerm(nextNodeIndex)
                    entries = []
                    if nextNodeIndex <= self.__getCurrentLogIndex():
                        entries = self.__getEntries(nextNodeIndex, self.__conf.appendEntriesBatchSize)
                        self.__raftNextIndex[nodeAddr] = entries[-1][1] + 1

                    message = {
                        'type': 'append_entries',
                        'term': self.__raftCurrentTerm,
                        'commit_index': self.__raftCommitIndex,
                        'entries': entries,
                        'prevLogIdx': prevLogIdx,
                        'prevLogTerm': prevLogTerm,
                    }
                    node.send(message)
                else:
                    transmissionData = self.__serializer.getTransmissionData(nodeAddr)
                    message = {
                        'type': 'append_entries',
                        'term': self.__raftCurrentTerm,
                        'commit_index': self.__raftCommitIndex,
                        'serialized': transmissionData,
                    }
                    node.send(message)
                    if transmissionData is not None:
                        isLast = transmissionData[2]
                        if isLast:
                            self.__raftNextIndex[nodeAddr] = self.__raftLog[0][1]
                            sendingSerialized = False
                        else:
                            sendingSerialized = True
                    else:
                        sendingSerialized = False

                nextNodeIndex = self.__raftNextIndex[nodeAddr]

                sendSingle = False

                delta = time.time() - startTime
                if delta > self.__conf.appendEntriesPeriod:
                    break

    def __send(self, nodeAddr, message):
        for node in self.__nodes:
            if node.getAddress() == nodeAddr:
                node.send(message)
                break

    def _getSelfNodeAddr(self):
        return self.__selfNodeAddr

    def _getConf(self):
        return self.__conf

    def _getEncryptor(self):
        return self.__encryptor

    def __tryLogCompaction(self):
        currTime = time.time()
        serializeState, serializeID = self.__serializer.checkSerializing()

        if serializeState == SERIALIZER_STATE.SUCCESS:
            self.__lastSerializedTime = currTime
            self.__deleteEntriesTo(serializeID)

        if serializeState == SERIALIZER_STATE.FAILED:
            LOG_WARNING("Failed to store full dump")

        if serializeState != SERIALIZER_STATE.NOT_SERIALIZING:
            return

        if len(self.__raftLog) <= self.__conf.logCompactionMinEntries and \
                currTime - self.__lastSerializedTime <= self.__conf.logCompactionMinTime and\
                not self.__forceLogCompaction:
            return

        self.__forceLogCompaction = False

        lastAppliedEntries = self.__getEntries(self.__raftLastApplied - 1, 2)
        if not lastAppliedEntries:
            return

        data = dict([(k, self.__dict__[k]) for k in list(self.__dict__.keys()) if k not in self.__properies])
        self.__serializer.serialize((data, lastAppliedEntries[1], lastAppliedEntries[0]), lastAppliedEntries[1][1])

    def __loadDumpFile(self):
        try:
            data = self.__serializer.deserialize()
            for k, v in data[0].items():
                self.__dict__[k] = v
            self.__raftLog = [data[2], data[1]]
            self.__raftLastApplied = data[1][1]
        except:
            LOG_WARNING('Failed to load full dump')
            LOG_CURRENT_EXCEPTION()


def replicated(func):
    def newFunc(self, *args, **kwargs):
        if kwargs.pop('_doApply', False):
            return func(self, *args, **kwargs)
        else:
            callback = kwargs.pop('callback', None)
            if kwargs:
                cmd = (self._methodToID[func.__name__], args, kwargs)
            elif args and not kwargs:
                cmd = (self._methodToID[func.__name__], args)
            else:
                cmd = self._methodToID[func.__name__]
            self._applyCommand(cmd, callback)
    return newFunc
