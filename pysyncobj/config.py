
class FAIL_REASON:
    SUCCESS = 0
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

        # Maximum number of commands processed in a single tick.
        self.maxCommandsPerTick = kwargs.get('maxCommandsPerTick', 100)

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
