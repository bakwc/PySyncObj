import time
import random
import os
import select
import socket
import cPickle
import zlib
import gzip
import struct
import threading
import Queue
import weakref
import collections
from debug_utils import *


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
			LOG_WARNING('failed to resolve host %s' % hostname)
			ips = []
		return ips


class _POLL_EVENT_TYPE:
	READ = 1
	WRITE = 2
	ERROR = 4

class _Poller(object):

	def subscribe(self, descr, callback, eventMask):
		pass

	def unsubscribe(self, descr):
		pass

	def poll(self, timeout):
		pass

class _SelectPoller(_Poller):

	def __init__(self):
		self.__descrsRead = set()
		self.__descrsWrite = set()
		self.__descrsError = set()
		self.__descrToCallbacks = {}


	def subscribe(self, descr, callback, eventMask):
		if eventMask & _POLL_EVENT_TYPE.READ:
			self.__descrsRead.add(descr)
		if eventMask & _POLL_EVENT_TYPE.WRITE:
			self.__descrsWrite.add(descr)
		if eventMask & _POLL_EVENT_TYPE.ERROR:
			self.__descrsError.add(descr)
		self.__descrToCallbacks[descr] = callback

	def unsubscribe(self, descr):
		self.__descrsRead.discard(descr)
		self.__descrsWrite.discard(descr)
		self.__descrsError.discard(descr)
		self.__descrToCallbacks.pop(descr, None)

	def poll(self, timeout):
		rlist, wlist, xlist = select.select(list(self.__descrsRead),
											list(self.__descrsWrite),
											list(self.__descrsError),
											timeout)

		allDescrs = set(rlist + wlist + xlist)
		rlist = set(rlist)
		wlist = set(wlist)
		xlist = set(xlist)
		for descr in allDescrs:
			event = 0
			if descr in rlist:
				event |= _POLL_EVENT_TYPE.READ
			if descr in wlist:
				event |= _POLL_EVENT_TYPE.WRITE
			if descr in xlist:
				event |= _POLL_EVENT_TYPE.ERROR
			self.__descrToCallbacks[descr](descr, event)

class _PollPoller(_Poller):

	def __init__(self):
		self.__poll = select.poll()
		self.__descrToCallbacks = {}

	def subscribe(self, descr, callback, eventMask):
		pollEventMask = 0
		if eventMask & _POLL_EVENT_TYPE.READ:
			pollEventMask |= select.POLLIN
		if eventMask & _POLL_EVENT_TYPE.WRITE:
			pollEventMask |= select.POLLOUT
		if eventMask & _POLL_EVENT_TYPE.ERROR:
			pollEventMask |= select.POLLERR
		self.__descrToCallbacks[descr] = callback
		self.__poll.register(descr, pollEventMask)

	def unsubscribe(self, descr):
		try:
			self.__poll.unregister(descr)
		except KeyError:
			pass

	def poll(self, timeout):
		events = self.__poll.poll(timeout)
		for descr, event in events:
			eventMask = 0
			if event & select.POLLIN:
				eventMask |= _POLL_EVENT_TYPE.READ
			if event & select.POLLOUT:
				eventMask |= _POLL_EVENT_TYPE.WRITE
			if event & select.POLLERR:
				eventMask |= _POLL_EVENT_TYPE.ERROR
			self.__descrToCallbacks[descr](descr, eventMask)

def createPoller():
	if hasattr(select, 'poll'):
		return _PollPoller()
	return _SelectPoller()


class _Connection(object):

	def __init__(self, socket = None, timeout = 10.0):
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
		self.__poller = syncObj._getPoller()
		self.__conn = _Connection(socket=None,
								  timeout=syncObj._getConf().connectionTimeout)

		self.__shouldConnect = syncObj._getSelfNodeAddr() > nodeAddr
		self.__lastConnectAttemptTime = 0
		self.__lastPingTime = 0
		self.__status = _NODE_STATUS.DISCONNECTED

	def __del__(self):
		self.__conn = None

	def onPartnerConnected(self, conn):
		self.__conn = conn
		self.__status = _NODE_STATUS.CONNECTED
		self.__poller.subscribe(self.__conn.fileno(),
								self.__processConnection,
								_POLL_EVENT_TYPE.READ | _POLL_EVENT_TYPE.WRITE | _POLL_EVENT_TYPE.ERROR)

	def getStatus(self):
		return self.__status

	def isConnected(self):
		return self.__status == _NODE_STATUS.CONNECTED

	def getAddress(self):
		return self.__nodeAddr

	def getSendBufferSize(self):
		return self.__conn.getSendBufferSize()

	def send(self, message):
		if self.__status != _NODE_STATUS.CONNECTED:
			return False
		self.__conn.send(message)
		if self.__conn.isDisconnected():
			self.__status = _NODE_STATUS.DISCONNECTED
			self.__poller.unsubscribe(self.__conn.fileno())
			self.__conn.close()
			return False
		return True

	def connectIfRequired(self):
		if not self.__shouldConnect:
			return
		if self.__status != _NODE_STATUS.DISCONNECTED:
			return
		if time.time() - self.__lastConnectAttemptTime < self.__syncObj()._getConf().connectionRetryTime:
			return
		self.__status = _NODE_STATUS.CONNECTING
		self.__lastConnectAttemptTime = time.time()
		if not self.__conn.connect(self.__ip, self.__port):
			self.__status = _NODE_STATUS.DISCONNECTED
			return
		self.__poller.subscribe(self.__conn.fileno(),
								self.__processConnection,
								_POLL_EVENT_TYPE.READ | _POLL_EVENT_TYPE.WRITE | _POLL_EVENT_TYPE.ERROR)

	def __processConnection(self, descr, eventType):
		assert descr == self.__conn.fileno()

		isError = False
		if eventType & _POLL_EVENT_TYPE.ERROR:
			isError = True

		if eventType & _POLL_EVENT_TYPE.READ or eventType & _POLL_EVENT_TYPE.WRITE:
			if self.__conn.socket().getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
				isError = True
			else:
				if self.__status == _NODE_STATUS.CONNECTING:
					self.__conn.send(self.__syncObj()._getSelfNodeAddr())
					self.__status = _NODE_STATUS.CONNECTED

		if isError or self.__conn.isDisconnected():
			self.__status = _NODE_STATUS.DISCONNECTED
			self.__conn.close()
			self.__poller.unsubscribe(descr)
			return

		if eventType & _POLL_EVENT_TYPE.WRITE:
			if self.__status == _NODE_STATUS.CONNECTING:
				self.__conn.send(self.__syncObj()._getSelfNodeAddr())
				self.__status = _NODE_STATUS.CONNECTED
			self.__conn.trySendBuffer()
			event = _POLL_EVENT_TYPE.READ | _POLL_EVENT_TYPE.ERROR
			if self.__conn.getSendBufferSize() > 0:
				event |= _POLL_EVENT_TYPE.WRITE
			if not self.__conn.isDisconnected():
				self.__poller.subscribe(descr, self.__processConnection, event)

		if eventType & _POLL_EVENT_TYPE.READ:
			self.__conn.read()
			while True:
				message = self.__conn.getMessage()
				if message is None:
					break
				self.__syncObj()._onMessageReceived(self.__nodeAddr, message)


class _SERIALIZER_STATE:
	NOT_SERIALIZING = 0
	SERIALIZING = 1
	SUCCESS = 2
	FAILED = 3

class _Serializer(object):

	def __init__(self, fileName, transmissionBatchSize):
		self.__fileName = fileName
		self.__transmissionBatchSize = transmissionBatchSize
		self.__pid = 0
		self.__currentID = 0
		self.__transmissions = {}
		self.__incomingTransmissionFile = None
		self.__inMemorySerializedData = None

	def checkSerializing(self):
		# In-memory case
		if self.__fileName is None:
			if self.__pid == -1:
				self.__pid = 0
				self.__transmissions = {}
				return _SERIALIZER_STATE.SUCCESS, self.__currentID
			return _SERIALIZER_STATE.NOT_SERIALIZING, None

		# File case
		pid = self.__pid
		if pid == 0:
			return _SERIALIZER_STATE.NOT_SERIALIZING, None
		try:
			rpid, status = os.waitpid(pid, os.WNOHANG)
		except OSError:
			self.__pid = 0
			return _SERIALIZER_STATE.FAILED, self.__currentID
		if rpid == pid:
			if status == 0:
				self.__transmissions = {}
				self.__pid = 0
				return _SERIALIZER_STATE.SUCCESS, self.__currentID
			self.__pid = 0
			return _SERIALIZER_STATE.FAILED, self.__currentID
		return _SERIALIZER_STATE.SERIALIZING, self.__currentID

	def serialize(self, data, id):
		if self.__pid != 0:
			return

		self.__currentID = id

		# In-memory case
		if self.__fileName is None:
			self.__inMemorySerializedData = zlib.compress(cPickle.dumps(data, -1))
			self.__pid = -1
			return

		# File case
		pid = os.fork()
		if pid != 0:
			self.__pid = pid
			return

		try:
			tmpFile = self.__fileName + '.tmp'
			with open(tmpFile, 'wb') as f:
				with gzip.GzipFile(fileobj=f) as g:
					cPickle.dump(data, g, -1)
			os.rename(tmpFile, self.__fileName)
			os._exit(0)
		except:
			os._exit(-1)

	def deserialize(self):
		if self.__fileName is None:
			return cPickle.loads(zlib.decompress(self.__inMemorySerializedData))

		with open(self.__fileName, 'rb') as f:
			with gzip.GzipFile(fileobj=f) as g:
				return cPickle.load(g)

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
				self.__incomingTransmissionFile = ''
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
				os.rename(tmpFile, self.__fileName)
			except:
				LOG_WARNING('Failed to rename temporary incoming transition file')
				LOG_CURRENT_EXCEPTION()
				return False
			return True
		return False

	def cancelTransmisstion(self, id):
		self.__transmissions.pop(id, None)


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

		# Will try to bind port every bindRetryTime seconds until success.
		self.bindRetryTime = kwargs.get('bindRetryTime', 1.0)


# http://ramcloud.stanford.edu/raft.pdf

class SyncObj(object):

	def __init__(self, selfNodeAddr, otherNodesAddrs, conf = None):

		if conf is None:
			self.__conf = SyncObjConf()
		else:
			self.__conf = conf

		self.__selfNodeAddr = selfNodeAddr
		self.__otherNodesAddrs = otherNodesAddrs
		self.__unknownConnections = {} # descr => _Connection
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
		self.__lastSerializedTime = 0
		self.__socket = None
		self.__resolver = _DnsCachingResolver(self.__conf.dnsCacheTime, self.__conf.dnsFailCacheTime)
		self.__serializer = _Serializer(self.__conf.fullDumpFile, self.__conf.logCompactionBatchSize)
		self.__poller = createPoller()
		self.__isInitialized = False
		self.__lastInitTryTime = 0

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
		try:
			self.__lastInitTryTime = time.time()
			self.__bind()
			self.__nodes = []
			for nodeAddr in self.__otherNodesAddrs:
				self.__nodes.append(_Node(self, nodeAddr))
				self.__raftNextIndex[nodeAddr] = 0
				self.__raftMatchIndex[nodeAddr] = 0
			self.__needLoadDumpFile = True
			self.__isInitialized = True
		except:
			LOG_CURRENT_EXCEPTION()

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

		self.__poller.poll(timeToWait)


	def _getLastCommitIndex(self):
		return self.__raftCommitIndex

	def printStatus(self):
		LOG_DEBUG('self', self.__selfNodeAddr)
		LOG_DEBUG('leader', self.__raftLeader)
		LOG_DEBUG('partner nodes', len(self.__nodes))
		for n in self.__nodes:
			LOG_DEBUG(n.getAddress(), n.getStatus())
		LOG_DEBUG('log size:', len(zlib.compress(cPickle.dumps(self.__raftLog, -1))))

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

	def __sendNextNodeIdx(self, nodeAddr, reset = False):
		self.__send(nodeAddr, {
			'type': 'next_node_idx',
			'next_node_idx': self.__getCurrentLogIndex() + 1,
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
		self.__poller.subscribe(self.__socket.fileno(),
								self.__onNewConnection,
								_POLL_EVENT_TYPE.READ | _POLL_EVENT_TYPE.ERROR)

	def __onNewConnection(self, localDescr, event):
		if event & _POLL_EVENT_TYPE.READ:
			try:
				sock, addr = self.__socket.accept()
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.__conf.sendBufferSize)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.__conf.recvBufferSize)
				sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
				sock.setblocking(0)
				conn = _Connection(socket=sock, timeout=self.__conf.connectionTimeout)
				descr = conn.fileno()
				self.__unknownConnections[descr] = conn
				self.__poller.subscribe(descr,
										self.__processUnknownConnections,
										_POLL_EVENT_TYPE.READ | _POLL_EVENT_TYPE.ERROR)
			except socket.error as e:
				if e.errno != socket.errno.EAGAIN:
					self.__isInitialized = False
					LOG_WARNING('Error in main socket:' + str(e))

		if event & _POLL_EVENT_TYPE.ERROR:
			self.__isInitialized = False
			LOG_WARNING('Error in main socket')

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
				self.__serializer.cancelTransmisstion(nodeAddr)
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

	def __processUnknownConnections(self, descr, event):
		conn = self.__unknownConnections[descr]
		partnerNode = None
		remove = False
		if event & _POLL_EVENT_TYPE.READ:
			conn.read()
			nodeAddr = conn.getMessage()
			if nodeAddr is not None:
				for node in self.__nodes:
					if node.getAddress() == nodeAddr:
						partnerNode = node
						break
				else:
					remove = True

		if event & _POLL_EVENT_TYPE.ERROR:
			remove = True

		if remove or conn.isDisconnected():
			self.__unknownConnections.pop(descr)
			self.__poller.unsubscribe(descr)
			conn.close()
			return

		if partnerNode is not None:
			self.__unknownConnections.pop(descr)
			assert conn.fileno() is not None
			partnerNode.onPartnerConnected(conn)

	def _getSelfNodeAddr(self):
		return self.__selfNodeAddr

	def _getConf(self):
		return self.__conf

	def _getResolver(self):
		return self.__resolver

	def _getPoller(self):
		return self.__poller

	def __tryLogCompaction(self):
		currTime = time.time()
		serializeState, serializeID = self.__serializer.checkSerializing()

		if serializeState == _SERIALIZER_STATE.SUCCESS:
			self.__lastSerializedTime = currTime
			self.__deleteEntriesTo(serializeID)

		if serializeState == _SERIALIZER_STATE.FAILED:
			LOG_WARNING("Failed to store full dump")

		if serializeState != _SERIALIZER_STATE.NOT_SERIALIZING:
			return

		if len(self.__raftLog) <= self.__conf.logCompactionMinEntries and \
				currTime - self.__lastSerializedTime <= self.__conf.logCompactionMinTime:
			return

		lastAppliedEntries = self.__getEntries(self.__raftLastApplied - 1, 2)
		if not lastAppliedEntries:
			return

		data = dict([(k, self.__dict__[k]) for k in self.__dict__.keys() if k not in self.__properies])
		self.__serializer.serialize((data, lastAppliedEntries[1], lastAppliedEntries[0]), lastAppliedEntries[1][1])

	def __loadDumpFile(self):
		try:
			data = self.__serializer.deserialize()
			for k, v in data[0].iteritems():
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
