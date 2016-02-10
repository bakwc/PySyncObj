import time
import random
import os
import select
import socket
import cPickle
import zlib
import struct
import threading
import Queue
import weakref
import collections


class _DnsCachingResolver(object):

	def __init__(self, cacheTime, failCacheTime):
		self.__cache = {}
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

	def __doResolve(self, hostname):
		try:
			ips = socket.gethostbyname_ex(hostname)[2]
		except socket.gaierror:
			print 'failed to resolve host %s' % hostname
			ips = []
		return ips


class _Connection(object):

	def __init__(self, socket = None, timeout = 10.0):
		self.__socket = socket
		self.__readBuffer = ''
		self.__writeBuffer = ''
		self.__lastReadTime = time.time()
		self.__timeout = timeout
		self.__disconnected = False

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
			return
		if self.__socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
			self.__disconnected = True
			return
		if not incoming:
			return
		self.__lastReadTime = time.time()
		self.__readBuffer += incoming

	def getMessage(self):
		if len(self.__readBuffer) < 4:
			return None
		l = struct.unpack('i', self.__readBuffer[:4])[0]
		if len(self.__readBuffer) - 4 < l:
			return None
		data = self.__readBuffer[4:4 + l]
		message = cPickle.loads(zlib.decompress(data))
		self.__readBuffer = self.__readBuffer[4 + l:]
		return message

	def close(self):
		self.__socket.close()

	def socket(self):
		return self.__socket


class _NODE_STATUS:
	DISCONNECTED = 0
	CONNECTING = 1
	CONNECTED = 2

class _Node(object):

	def __init__(self, syncObj, nodeAddr):
		self.__syncObj = weakref.ref(syncObj)
		self.__nodeAddr = nodeAddr
		self.__ip = syncObj._getResolver().resolve(nodeAddr.split(':')[0])
		self.__port = int(nodeAddr.split(':')[1])
		self.__conn = _Connection(syncObj._getConf().connectionTimeout)
		self.__shouldConnect = syncObj._getSelfNodeAddr() > nodeAddr
		self.__lastConnectAttemptTime = 0
		self.__lastPingTime = 0
		self.__status = _NODE_STATUS.DISCONNECTED

	def __del__(self):
		self.__conn = None

	def onPartnerConnected(self, conn):
		self.__conn = conn
		self.__status = _NODE_STATUS.CONNECTED

	def onTickStage1(self):
		if self.__shouldConnect:
			if self.__status == _NODE_STATUS.DISCONNECTED:
				self.__connect()

		if self.__shouldConnect and self.__status == _NODE_STATUS.CONNECTING:
			return (self.__conn.socket(), self.__conn.socket(), self.__conn.socket())
		if self.__status == _NODE_STATUS.CONNECTED:
			readyWriteSocket = None
			if self.__conn.getSendBufferSize() > 0:
				readyWriteSocket = self.__conn.socket()
			return (self.__conn.socket(), readyWriteSocket, self.__conn.socket())
		return None

	def getStatus(self):
		return self.__status

	def isConnected(self):
		return self.__status == _NODE_STATUS.CONNECTED

	def getAddress(self):
		return self.__nodeAddr

	def getSendBufferSize(self):
		return self.__conn.getSendBufferSize()

	def onTickStage2(self, rlist, wlist, xlist):

		if self.__shouldConnect:
			if self.__status == _NODE_STATUS.CONNECTING:
				self.__checkConnected(rlist, wlist, xlist)

		if self.__status == _NODE_STATUS.CONNECTED:
			if self.__conn.socket() in rlist:
				self.__conn.read()
				while True:
					message = self.__conn.getMessage()
					if message is None:
						break
					self.__syncObj()._onMessageReceived(self.__nodeAddr, message)
			if self.__conn.socket() in wlist:
				self.__conn.trySendBuffer()

			if self.__conn.socket() in xlist or self.__conn.isDisconnected():
				self.__status = _NODE_STATUS.DISCONNECTED
				self.__conn.close()

	def send(self, message):
		if self.__status != _NODE_STATUS.CONNECTED:
			return False
		self.__conn.send(message)
		if self.__conn.isDisconnected():
			self.__status = _NODE_STATUS.DISCONNECTED
			self.__conn.close()
			return False
		return True

	def __connect(self):
		if time.time() - self.__lastConnectAttemptTime < self.__syncObj()._getConf().connectionRetryTime:
			return

		self.__status = _NODE_STATUS.CONNECTING

		self.__lastConnectAttemptTime = time.time()

		if not self.__conn.connect(self.__ip, self.__port):
			self.__status = _NODE_STATUS.DISCONNECTED

	def __checkConnected(self, rlist, wlist, xlist):
		if self.__conn.socket() in xlist:
			self.__status = _NODE_STATUS.DISCONNECTED
		if self.__conn.socket() in rlist or self.__conn.socket() in wlist:
			if self.__conn.socket().getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
				self.__status = _NODE_STATUS.DISCONNECTED
			else:
				self.__status = _NODE_STATUS.CONNECTED
				self.__conn.send(self.__syncObj()._getSelfNodeAddr())


class _RAFT_STATE:
	FOLLOWER = 0
	CANDIDATE = 1
	LEADER = 2

class FAIL_REASON:
	SUCCESS	= 0
	QUEUE_FULL = 1
	MISSING_LEADER = 2
	DISCARDED = 3
	NOT_LEADER = 4
	LEADER_CHANGED = 5

class SyncObjConf(object):
	def __init__(self, **kwargs):
		# Disable autoTick if you want to call onTick manually.
		# Otherwise it will be called automatically from separate thread.
		self.autoTick = kwargs.get('autoTick', True)
		self.autoTickPeriod = kwargs.get('autoTickPeriod', 0.05)

		# Commands queue is used to store commands before real processing.
		self.commandsQueueSize = kwargs.get('commandsQueueSize', 1000)

		# After randomly selected timeout (in range from minTimeout to maxTimeout)
		# leader considered dead, and leader election starts.
		self.raftMinTimeout = kwargs.get('raftMinTimeout', 1.0)
		self.raftMaxTimeout = kwargs.get('raftMaxTimeout', 3.0)

		# Interval of sending append_entries (ping) command.
		# Should be less then raftMinTimeout.
		self.appendEntriesPeriod = kwargs.get('appendEntriesPeriod', 0.3)

		# When no data received for connectionTimeout - connection considered dead.
		# Should be more then raftMaxTimeout.
		self.connectionTimeout = kwargs.get('connectionTimeout', 3.5)

		# Interval between connection attempts.
		# Will try to connect to offline nodes each connectionRetryTime.
		self.connectionRetryTime = kwargs.get('connectionRetryTime', 5.0)

		# Max number of log entries per single append_entries command.
		self.appendEntriesBatchSize = kwargs.get('appendEntriesBatchSize', 1000)

		# Send multiple entries in a single command.
		# Enabled (default) - improve overal perfomence (requests per second)
		# Disabled - improve single request speed (don't wait till batch ready)
		self.appendEntriesUseBatch = kwargs.get('appendEntriesUseBatch', True)

		# Size of receive and send buffer for sockets.
		self.sendBufferSize = kwargs.get('sendBufferSize', 2 ** 13)
		self.recvBufferSize = kwargs.get('recvBufferSize', 2 ** 13)

		# Time to cache dns requests (improves performance,
		# no need to resolve address for each connection attempt).
		self.dnsCacheTime = kwargs.get('dnsCacheTime', 600.0)
		self.dnsFailCacheTime = kwargs.get('dnsFailCacheTime', 30.0)

		# Log will be compacted after it reach minEntries size or
		# minTime after previous compaction.
		self.logCompactionMinEntries = kwargs.get('logCompactionMinEntries', 5000)
		self.logCompactionMinTime = kwargs.get('logCompactionMinTime', 60)

		# Max number of bytes per single append_entries command
		# while sending serialized object.
		self.logCompactionBatchSize = kwargs.get('logCompactionBatchSize', 2 ** 13)

		# If true - commands will be enqueued and executed after leader detected.
		# Otherwise - FAIL_REASON.MISSING_LEADER error will be emitted.
		# Leader is missing when esteblishing connection or when election in progress.
		self.commandsWaitLeader = kwargs.get('commandsWaitLeader', True)

		# File to store full serialized object. Save full dump on disc when doing log compaction.
		# None - to disable store.
		self.fullDumpFile = kwargs.get('fullDumpFile', None)



class SyncObj(object):

	def __init__(self, selfNodeAddr, otherNodesAddrs, conf = None):

		if conf is None:
			self.__conf = SyncObjConf()
		else:
			self.__conf = conf

		self.__selfNodeAddr = selfNodeAddr
		self.__otherNodesAddrs = otherNodesAddrs
		self.__unknownConnections = []
		self.__raftState = _RAFT_STATE.FOLLOWER
		self.__raftCurrentTerm = 0
		self.__votesCount = 0
		self.__raftLeader = None
		self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
		self.__raftLog = [] # (command, logID, term)
		self.__raftLog.append((None, 1, self.__raftCurrentTerm))
		self.__raftCommitIndex = 1
		self.__raftLastApplied = 1
		self.__raftNextIndex = {}
		self.__raftMatchIndex = {}
		self.__lastSerialized = None
		self.__lastSerializedTime = 0
		self.__outgoingSerializedData = {}
		self.__incomingSerializedData = None
		self.__socket = None

		self._methodToID = {}
		self._idToMethod = {}
		methods = sorted([m for m in dir(self) if callable(getattr(self, m))])
		for i, method in enumerate(methods):
			self._methodToID[method] = i
			self._idToMethod[i] = getattr(self, method)

		self.__thread = None
		self.__mainThread = None
		self.__initialised = None
		self.__commandsQueue = Queue.Queue(self.__conf.commandsQueueSize)
		self.__resolver = None
		self.__nodes = []
		self.__newAppendEntriesTime = 0

		self.__commandsWaitingCommit = collections.defaultdict(list) # logID => [(termID, callback), ...]
		self.__commandsLocalCounter = 0
		self.__commandsWaitingReply = {} # commandLocalCounter => callback

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
		self.__resolver = _DnsCachingResolver(self.__conf.dnsCacheTime, self.__conf.dnsFailCacheTime)
		self.__bind()
		for nodeAddr in self.__otherNodesAddrs:
			self.__nodes.append(_Node(self, nodeAddr))
			self.__raftNextIndex[nodeAddr] = 0
			self.__raftMatchIndex[nodeAddr] = 0
		self.__needLoadDumpFile = True

	def _applyCommand(self, command, callback):
		try:
			self.__commandsQueue.put_nowait((command, callback))
		except Queue.Full:
			callback(None, FAIL_REASON.QUEUE_FULL)

	def _checkCommandsToApply(self):
		while True:
			if self.__raftLeader is None and self.__conf.commandsWaitLeader:
				break
			try:
				command, callback = self.__commandsQueue.get_nowait()
			except Queue.Empty:
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

	def _onTick(self, timeToWait = 0.0):

		if self.__needLoadDumpFile:
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
						'last_log_index': self.__raftLog[-1][1],
						'last_log_term': self.__raftLog[-1][2],
					})
				self.__onLeaderChanged()

		if self.__raftState == _RAFT_STATE.LEADER:
			while self.__raftCommitIndex < self.__raftLog[-1][1]:
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

		socketsToCheckR = [self.__socket]
		socketsToCheckW = [self.__socket]
		socketsToCheckX = [self.__socket]
		for conn in self.__unknownConnections:
			socketsToCheckR.append(conn.socket())
			socketsToCheckX.append(conn.socket())

		for node in self.__nodes:
			socks = node.onTickStage1()
			if socks is not None:
				sockR, sockW, sockX = socks
				if sockR is not None:
					socketsToCheckR.append(sockR)
				if sockW is not None:
					socketsToCheckW.append(sockW)
				if sockX is not None:
					socketsToCheckX.append(sockX)

		rlist, wlist, xlist = select.select(socketsToCheckR, socketsToCheckW, socketsToCheckX, timeToWait)

		if self.__socket in rlist:
			try:
				sock, addr = self.__socket.accept()
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.__conf.sendBufferSize)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.__conf.recvBufferSize)
				sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
				sock.setblocking(0)
				self.__unknownConnections.append(_Connection(sock, self.__conf.connectionTimeout))
			except socket.error as e:
				if e.errno != socket.errno.EAGAIN:
					raise e

		if self.__socket in xlist:
			print ' === some error for main socket'
			# todo: handle

		self.__processUnknownConnections(rlist, xlist)

		for node in self.__nodes:
			node.onTickStage2(rlist, wlist, xlist)

	def _getLastCommitIndex(self):
		return self.__raftCommitIndex

	def printStatus(self):
		print 'self:    ', self.__selfNodeAddr
		print 'leader:  ', self.__raftLeader
		print 'partner nodes:', len(self.__nodes)
		for n in self.__nodes:
			print n.getAddress(), n.getStatus()
		print 'log size:', len(zlib.compress(cPickle.dumps(self.__raftLog, -1)))
		print ''

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
					if lastLogTerm < self.__raftLog[-1][2]:
						return
					if lastLogTerm == self.__raftLog[-1][2] and \
							lastLogIdx < self.__raftLog[-1][1]:
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

			# Regular appen entries
			if serialized is None:
				prevLogIdx = message['prevLogIdx']
				prevLogTerm = message['prevLogTerm']
				prevEntries = self.__getEntries(prevLogIdx)
				if not prevEntries:
					self.__sendNextNodeIdx(nodeAddr, reset = True)
					return
				if prevEntries[0][2] != prevLogTerm:
					self.__deleteEntriesFrom(prevLogIdx)
					self.__sendNextNodeIdx(nodeAddr, reset = True)
					return
				if len(prevEntries) > 1:
					self.__deleteEntriesFrom(prevLogIdx + 1)
				self.__raftLog += newEntries

			# Install snapshot
			else:
				isLast = message.get('is_last')
				isFirst = message.get('is_first')
				if isFirst:
					self.__incomingSerializedData = ''
				self.__incomingSerializedData += serialized
				if isLast:
					self.__lastSerialized = self.__incomingSerializedData
					self.__incomingSerializedData = ''
					self._deserialize()

			self.__sendNextNodeIdx(nodeAddr)

			self.__raftCommitIndex = min(leaderCommitIndex, self.__raftLog[-1][1])

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

	def __sendNextNodeIdx(self, nodeAddr, reset = False):
		self.__send(nodeAddr, {
			'type': 'next_node_idx',
			'next_node_idx': self.__raftLog[-1][1] + 1,
			'reset': reset,
		})

	def __generateRaftTimeout(self):
		minTimeout = self.__conf.raftMinTimeout
		maxTimeout = self.__conf.raftMaxTimeout
		return minTimeout + (maxTimeout - minTimeout) * random.random()

	def __bind(self):
		self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.__conf.sendBufferSize)
		self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.__conf.recvBufferSize)
		self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
		self.__socket.setblocking(0)
		host, port = self.__selfNodeAddr.split(':')
		self.__socket.bind((host, int(port)))
		self.__socket.listen(5)

	def __getCurrentLogIndex(self):
		return self.__raftLog[-1][1]

	def __getPrevLogIndexTerm(self, nextNodeIndex):
		prevIndex = nextNodeIndex - 1
		entries = self.__getEntries(prevIndex, 1)
		if entries:
			return prevIndex, entries[0][2]
		return None, None

	def __getEntries(self, fromIDx, count = None):
		firstEntryIDx = self.__raftLog[0][1]
		if fromIDx < firstEntryIDx:
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
				self.__outgoingSerializedData.pop(nodeAddr, 0)
				continue

			sendSingle = True
			sendingSerialized = False
			nextNodeIndex = self.__raftNextIndex[nodeAddr]
			entries = []

			while nextNodeIndex <= self.__getCurrentLogIndex() or sendSingle or sendingSerialized:
				if nextNodeIndex >= self.__raftLog[0][1]:
					prevLogIdx, prevLogTerm = self.__getPrevLogIndexTerm(nextNodeIndex)
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
					alreadyTansmitted = self.__outgoingSerializedData.get(nodeAddr, 0)
					currentChunk = self.__lastSerialized[alreadyTansmitted:alreadyTansmitted +
																self.__conf.logCompactionBatchSize]
					isLast = alreadyTansmitted + len(currentChunk) == len(self.__lastSerialized)
					isFirst = alreadyTansmitted == 0
					message = {
						'type': 'append_entries',
						'term': self.__raftCurrentTerm,
						'commit_index': self.__raftCommitIndex,
						'serialized': currentChunk,
						'is_last': isLast,
						'is_first': isFirst,
					}
					node.send(message)
					if isLast:
						self.__raftNextIndex[nodeAddr] = self.__raftLog[0][1]
						self.__outgoingSerializedData.pop(nodeAddr, 0)
						sendingSerialized = False
					else:
						sendingSerialized = True
						self.__outgoingSerializedData[nodeAddr] = alreadyTansmitted + len(currentChunk)

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

	def __processUnknownConnections(self, rlist, xlist):
		newUnknownConnections = []
		for conn in self.__unknownConnections:
			remove = False
			if conn.socket() in rlist:
				conn.read()
				nodeAddr = conn.getMessage()
				if nodeAddr is not None:
					for node in self.__nodes:
						if node.getAddress() == nodeAddr:
							node.onPartnerConnected(conn)
							break
					remove = True
			if conn.socket() in xlist:
				remove = True
			if not remove and not conn.isDisconnected():
				newUnknownConnections.append(conn)

		self.__unknownConnections = newUnknownConnections

	def _getSelfNodeAddr(self):
		return self.__selfNodeAddr

	def _getConf(self):
		return self.__conf

	def _getResolver(self):
		return self.__resolver

	def __tryLogCompaction(self):
		currTime = time.time()
		if len(self.__raftLog) > self.__conf.logCompactionMinEntries or \
				currTime - self.__lastSerializedTime > self.__conf.logCompactionMinTime:
			if self._serialize():
				self.__lastSerializedTime = currTime

	def _serialize(self):
		lastAppliedEntries = self.__getEntries(self.__raftLastApplied - 1, 2)
		if not lastAppliedEntries:
			return False

		data = dict([(k, self.__dict__[k]) for k in self.__dict__.keys() if k not in self.__properies])
		self.__lastSerialized = zlib.compress(cPickle.dumps((data, lastAppliedEntries[1], lastAppliedEntries[0]), -1), 3)

		self.__deleteEntriesTo(lastAppliedEntries[1][1])
		self.__outgoingSerializedData = {}

		fullDumpFile = self.__conf.fullDumpFile
		if fullDumpFile is None:
			return True

		try:
			with open(fullDumpFile + '.tmp', 'wb') as f:
				f.write(self.__lastSerialized)
			os.rename(fullDumpFile + '.tmp', fullDumpFile)
		except Exception as e:
			print 'WARNING: failed to store full dump:', e
			return True

		return True

	def __loadDumpFile(self):
		if self.__conf.fullDumpFile is not None:
			if os.path.isfile(self.__conf.fullDumpFile):
				try:
					with open(self.__conf.fullDumpFile, 'rb') as f:
						self.__lastSerialized = f.read()
						self._deserialize()
				except Exception as e:
					self.__lastSerialized = None
					print 'WARNING: failed to load full dump:', e

	def _deserialize(self):
		try:
			data = cPickle.loads(zlib.decompress(self.__lastSerialized))
		except:
			return
		for k, v in data[0].iteritems():
			self.__dict__[k] = v
		self.__raftLog = [data[2], data[1]]
		self.__raftLastApplied = data[1][1]

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
