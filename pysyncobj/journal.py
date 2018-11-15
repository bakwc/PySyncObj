import hashlib
import os
import mmap
import pickle
import struct

from .atomic_replace import atomicReplace
from .version import VERSION
from .pickle import to_bytes

class Journal(object):

    def add(self, command, idx, term):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def deleteEntriesFrom(self, entryFrom):
        raise NotImplementedError

    def deleteEntriesTo(self, entryTo):
        raise NotImplementedError

    def __getitem__(self, item):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def _destroy(self):
        raise NotImplementedError


class MemoryJournal(Journal):

    def __init__(self):
        self.__journal = []
        self.__bytesSize = 0

    def add(self, command, idx, term):
        self.__journal.append((command, idx, term))

    def clear(self):
        self.__journal = []

    def deleteEntriesFrom(self, entryFrom):
        del self.__journal[entryFrom:]

    def deleteEntriesTo(self, entryTo):
        self.__journal = self.__journal[entryTo:]

    def __getitem__(self, item):
        return self.__journal[item]

    def __len__(self):
        return len(self.__journal)

    def _destroy(self):
        pass


class ResizableFile(object):

    def __init__(self, fileName, initialSize = 1024, resizeFactor = 2.0, defaultContent = None):
        self.__fileName = fileName
        self.__resizeFactor = resizeFactor
        if not os.path.exists(fileName):
            with open(fileName, 'wb') as f:
                if defaultContent is not None:
                    f.write(defaultContent)
        self.__f = open(fileName, 'r+b')
        self.__mm = mmap.mmap(self.__f.fileno(), 0)
        currSize = self.__mm.size()
        if currSize < initialSize:
            try:
                self.__mm.resize(initialSize)
            except SystemError:
                self.__extand(initialSize - currSize)

    def write(self, offset, values):
        size = len(values)
        currSize = self.__mm.size()
        while offset + size > self.__mm.size():
            try:
                self.__mm.resize(int(self.__mm.size() * self.__resizeFactor))
            except SystemError:
                self.__extand(int(self.__mm.size() * self.__resizeFactor) - currSize)
        self.__mm[offset:offset + size] = values

    def read(self, offset, size):
        return self.__mm[offset:offset + size]

    def __extand(self, bytesToAdd):
        self.__mm.close()
        self.__f.close()
        with open(self.__fileName, 'ab') as f:
            f.write(b'\0' * bytesToAdd)
        self.__f = open(self.__fileName, 'r+b')
        self.__mm = mmap.mmap(self.__f.fileno(), 0)

    def _destroy(self):
        self.__mm.flush()
        self.__mm.close()
        self.__f.close()

    def flush(self):
        self.__mm.flush()



JOURNAL_FORMAT_VERSION = 2
APP_NAME = b'PYSYNCOBJ'
APP_VERSION = str.encode(VERSION)

NAME_SIZE = 24
VERSION_SIZE = 8
assert len(APP_NAME) < NAME_SIZE
assert len(APP_VERSION) < VERSION_SIZE
CURRENT_TERM_SIZE = 8
VOTED_FOR_SIZE = 16
FIRST_RECORD_OFFSET = NAME_SIZE + VERSION_SIZE + 4 + CURRENT_TERM_SIZE + VOTED_FOR_SIZE + 4
LAST_RECORD_OFFSET_OFFSET = NAME_SIZE + VERSION_SIZE + 4 + CURRENT_TERM_SIZE + VOTED_FOR_SIZE

# Journal version 2:
#   APP_NAME (24b) + APP_VERSION (8b) + FORMAT_VERSION (4b) + CURRENT_TERM (8b) + VOTED_FOR (16b) + LAST_RECORD_OFFSET (4b) +
#       record1size + record1 + record1size   +  record2size + record2 + record2size   +  ...
#                 (record1)                   |               (record2)                |  ...

# VOTED_FOR is an MD5 hash of the pickled node ID.
# LAST_RECORD_OFFSET is the offset from the beginning of the journal file at which the last record ends.

# Version 1 is identical except it has neither CURRENT_TERM nor VOTED_FOR.

class FileJournal(Journal):

    def __init__(self, journalFile, flushJournal):
        self.__journalFile = ResizableFile(journalFile, defaultContent=self.__getDefaultHeader())
        self.__journal = []

        # Handle journal format version upgrades
        version = struct.unpack('<I', self.__journalFile.read(32, 4))[0]
        if version == 1:
            # Header size increased by 24 bytes, so everything needs to be moved...
            tmpFile = journalFile + '.tmp'
            if os.path.exists(tmpFile):
                raise RuntimeError('Migration of journal file failed: {} already exists'.format(tmpFile))
            oldJournalFile = self.__journalFile  # Just for readability
            newJournalFile = ResizableFile(tmpFile, defaultContent=self.__getDefaultHeader())
            oldFirstRecordOffset = NAME_SIZE + VERSION_SIZE + 4 + 4
            oldLastRecordOffset = struct.unpack('<I', oldJournalFile.read(NAME_SIZE + VERSION_SIZE + 4, 4))[0]
            oldCurrentOffset = oldFirstRecordOffset
            deltaOffset = 24  # delta in record offsets between old and new format
            while oldCurrentOffset < oldLastRecordOffset:
                # Copy data in chunks of 4 MB (plus possibly a smaller chunk at the end)
                d = oldJournalFile.read(oldCurrentOffset, min(4000000, oldLastRecordOffset - oldCurrentOffset))
                if not d:
                    # Reached EOF
                    break
                newJournalFile.write(oldCurrentOffset + deltaOffset, d)
                oldCurrentOffset += len(d)
            newJournalFile.write(LAST_RECORD_OFFSET_OFFSET, struct.pack('<I', oldLastRecordOffset + deltaOffset))
            newJournalFile.flush()

            del oldJournalFile  # Delete reference
            self.__journalFile._destroy()
            newJournalFile._destroy()
            atomicReplace(tmpFile, journalFile)
            self.__journalFile = ResizableFile(journalFile, defaultContent=self.__getDefaultHeader())
        elif version == JOURNAL_FORMAT_VERSION:
            # Nothing to do
            pass
        else:
            raise RuntimeError('Unknown journal file version encountered: {} (expected <= {})'.format(version, JOURNAL_FORMAT_VERSION))

        currentOffset = FIRST_RECORD_OFFSET
        lastRecordOffset = self.__getLastRecordOffset()
        while currentOffset < lastRecordOffset:
            nextRecordSize = struct.unpack('<I', self.__journalFile.read(currentOffset, 4))[0]
            nextRecordData = self.__journalFile.read(currentOffset + 4, nextRecordSize)
            command = nextRecordData[16:]
            idx, term = struct.unpack('<QQ', nextRecordData[:16])
            self.__journal.append((command, idx, term))
            currentOffset += nextRecordSize + 8
        self.__currentOffset = currentOffset
        self.__flushJournal = flushJournal

    def __getDefaultHeader(self):
        appName = APP_NAME + b'\0' * (NAME_SIZE - len(APP_NAME))
        appVersion = APP_VERSION + b'\0' * (VERSION_SIZE - len(APP_VERSION))
        header = (appName + appVersion + struct.pack('<I', JOURNAL_FORMAT_VERSION) +
          struct.pack('<Q', 0) + # default term = 0
          hashlib.md5(pickle.dumps(None)).digest() + # default voted for = empty
          struct.pack('<I', FIRST_RECORD_OFFSET))
        return header

    def __getLastRecordOffset(self):
        return struct.unpack('<I', self.__journalFile.read(LAST_RECORD_OFFSET_OFFSET, 4))[0]

    def __setLastRecordOffset(self, offset):
        self.__journalFile.write(LAST_RECORD_OFFSET_OFFSET, struct.pack('<I', offset))
        # No auto-flushing needed here because it's called in the methods below.

    def add(self, command, idx, term, _doFlush = True):
        self.__journal.append((command, idx, term))
        cmdData = struct.pack('<QQ', idx, term) + to_bytes(command)
        cmdLenData = struct.pack('<I', len(cmdData))
        cmdData = cmdLenData + cmdData + cmdLenData
        self.__journalFile.write(self.__currentOffset, cmdData)
        self.__currentOffset += len(cmdData)
        self.__setLastRecordOffset(self.__currentOffset)
        if _doFlush and self.__flushJournal:
            self.flush()

    def clear(self):
        self.__journal = []
        self.__setLastRecordOffset(FIRST_RECORD_OFFSET)
        self.__currentOffset = FIRST_RECORD_OFFSET
        if self.__flushJournal:
            self.flush()

    def __getitem__(self, idx):
        return self.__journal[idx]

    def __len__(self):
        return len(self.__journal)

    def deleteEntriesFrom(self, entryFrom):
        entriesToRemove = len(self.__journal) - entryFrom
        del self.__journal[entryFrom:]
        currentOffset = self.__currentOffset
        removedEntries = 0
        while removedEntries < entriesToRemove:
            prevRecordSize = struct.unpack('<I', self.__journalFile.read(currentOffset - 4, 4))[0]
            currentOffset -= prevRecordSize + 8
            removedEntries += 1
            if removedEntries % 10 == 0:
                self.__setLastRecordOffset(currentOffset)
        self.__currentOffset = currentOffset
        self.__setLastRecordOffset(currentOffset)
        if self.__flushJournal:
            self.flush()

    def deleteEntriesTo(self, entryTo):
        journal = self.__journal[entryTo:]
        self.clear()
        for entry in journal:
            self.add(*entry, _doFlush = False)
        if self.__flushJournal:
            self.flush()

    def _destroy(self):
        self.__journalFile._destroy()

    def flush(self):
        self.__journalFile.flush()

def createJournal(journalFile, flushJournal):
    if flushJournal is None:
        flushJournal = journalFile is not None
    if journalFile is None:
        assert not flushJournal
        return MemoryJournal()
    return FileJournal(journalFile, flushJournal)
