import pysyncobj
import pysyncobj.testrevision
import sys
import time

class MyCounter(pysyncobj.SyncObj):
	def __init__(self, selfAddr, otherAddrs, **kwargs):
		super(MyCounter, self).__init__(selfAddr, otherAddrs, **kwargs)
		self._counter = 0

	@pysyncobj.replicated
	def incCounter(self):
		self._counter += 1

	def getCounter(self):
		return self._counter


def main(argv = sys.argv[1:]):#, stdin = sys.stdin):
	selfAddr = argv[0]
	otherAddrs = argv[1:]
	conf = pysyncobj.SyncObjConf()
	conf.journalFile = './journal'
	conf.fullDumpFile = './dump'
	counter = MyCounter(selfAddr, otherAddrs, conf = conf)

	print('{} ready at {}'.format(selfAddr, pysyncobj.testrevision.rev), file = sys.stderr)

	while True:
		line = sys.stdin.readline().strip()

		if line == 'wait':
			time.sleep(2)
			print('waited', flush = True)
		elif line == 'increment':
			while True:
				try:
					counter.incCounter(sync = True)
				except pysyncobj.SyncObjException as e:
					print('{} increment yielded SyncObjException with error code {}, retrying'.format(selfAddr, e.errorCode), file = sys.stderr)
				else:
					break
			print('incremented', flush = True)
		elif line == 'print':
			print(counter.getCounter(), flush = True)
		elif line == 'printlog':
			print(repr(counter._SyncObj__raftLog[:]).replace('\n', '    '), flush = True)
		elif line == 'quit' or line == '':
			break
		else:
			print('Got unknown command: {}'.format(line), file = sys.stderr)


if __name__ == '__main__':
	main()
