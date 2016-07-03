# PySyncObj

[ ![Build Status] [travis-image] ] [travis] [ ![Release] [release-image] ] [releases] [ ![License] [license-image] ] [license]

[travis-image]: https://travis-ci.org/bakwc/PySyncObj.svg?branch=master
[travis]: https://travis-ci.org/bakwc/PySyncObj

[release-image]: https://img.shields.io/badge/release-0.1.8-blue.svg?style=flat
[releases]: https://github.com/bakwc/PySyncObj/releases

[license-image]: https://img.shields.io/badge/license-MIT-blue.svg?style=flat
[license]: LICENSE.txt

PySyncObj is a python library that provides ability to sync your data between multiple servers.
- It use [raft protocol](http://raft.github.io/) for leader election and log replication.
- It supports log compaction. It use fork for copy-on-write while serializing data on disk.
- It supports in-memory and on-disk serialization. You can use in-memory mode for small data and on-disk for big one.
- It has encryption - you can set password and use it in external network.
- It supports python2 and python3. No dependencies required (only optional one, eg. cryptography).
- Configurable event loop - it can works in separate thread with it's own event loop - or you can call onTick function inside your own one.
- Convenient interface - you can easily transform arbitrary class into a replicated one (see example below).

## Install
PySyncObj itself:
```bash
pip install pysyncobj
```
Cryptography for encryption (optional):
```bash
pip install cryptography
```

## Usage
Consider you have a class that implements counter:
```python
class MyCounter(object):
	def __init__(self):
		self.__counter = 0

	def incCounter(self):
		self.__counter += 1

	def getCounter(self):
		return self.__counter
```
So, to transform your class into a replicated one:
 - Inherit it from SyncObj
 - Initialize SyncObj with a self address and a list of partner addresses. Eg. if you have `serverA`, `serverB` and `serverC` and want to use 4321 port, you should use self address `serverA:4321` with partners `[serverB:4321, serverC:4321]` for your application, running at `serverA`; self address `serverB:4321` with partners `[serverA:4321, serverC:4321]` for your application at `serverB`; self address `serverC:4321` with partners `[serverA:4321, serverB:4321]` for app at `serverC`.
 - Mark all your methods that modifies your class fields with `@replicated` decorator.
So your final class will looks like:
```python
class MyCounter(SyncObj):
	def __init__(self):
		super(MyCounter, self).__init__('serverA:4321', ['serverB:4321', 'serverC:4321'])
		self.__counter = 0

	@replicated
	def incCounter(self):
		self.__counter += 1

	def getCounter(self):
		return self.__counter
```
And thats all! Now you can call `incCounter` on `serverA`, and check counter value on `serverB` - they will be synchronized. You can look at examples and syncobj_ut.py for more use-cases.
