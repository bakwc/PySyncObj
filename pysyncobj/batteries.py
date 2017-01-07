from syncobj import SyncObjConsumer, replicated


class ReplCounter(SyncObjConsumer):
    def __init__(self):
        super(ReplCounter, self).__init__()
        self.__counter = 0

    @replicated
    def set(self, newValue):
        self.__counter = newValue
        return self.__counter

    @replicated
    def add(self, value):
        self.__counter += value
        return self.__counter

    @replicated
    def sub(self, value):
        self.__counter -= value
        return self.__counter

    @replicated
    def inc(self):
        self.__counter += 1
        return self.__counter

    def get(self):
        return self.__counter


class ReplList(SyncObjConsumer):
    def __init__(self):
        super(ReplList, self).__init__()
        self.__data = []

    @replicated
    def reset(self, newData):
        assert isinstance(newData, list)
        self.__data = newData

    @replicated
    def set(self, position, newValue):
        self.__data[position] = newValue

    @replicated
    def append(self, item):
        self.__data.append(item)

    @replicated
    def extend(self, other):
        self.__data.extend(other)

    @replicated
    def insert(self, position, element):
        self.__data.insert(position, element)

    @replicated
    def remove(self, element):
        self.__data.remove(element)

    @replicated
    def pop(self, position=None):
        return self.__data.pop(position)

    @replicated
    def sort(self, reverse=False):
        self.__data.sort(reverse=reverse)

    def index(self, element, start=None, stop=None):
        return self.__data.index(element, start, stop)

    def count(self, element):
        return self.__data.count(element)

    def get(self, position):
        return self.__data[position]

    def __getitem__(self, position):
        return self.__data[position]

    def __len__(self):
        return len(self.__data)

    def rawData(self):
        return self.__data


class ReplDict(SyncObjConsumer):
    def __init__(self):
        super(ReplDict, self).__init__()
        self.__data = {}

    @replicated
    def reset(self, newData):
        assert isinstance(newData, dict)
        self.__data = newData

    @replicated
    def __setitem__(self, key, value):
        self.__data[key] = value

    @replicated
    def set(self, key, value):
        self.__data[key] = value

    @replicated
    def setdefault(self, key, default):
        return self.__data.setdefault(key, default)

    @replicated
    def update(self, other):
        self.__data.update(other)

    @replicated
    def pop(self, key, default=None):
        return self.__data.pop(key, default)

    @replicated
    def clear(self):
        self.__data.clear()

    def __getitem__(self, item):
        return self.__data[item]

    def get(self, key, default=None):
        return self.__data.get(key, default)

    def __len__(self):
        return len(self.__data)

    def __contains__(self, item):
        return item in self.__data

    def keys(self):
        return self.__data.keys()

    def values(self):
        return self.__data.values()

    def items(self):
        return self.__data.items()

    def rawData(self):
        return self.__data


class ReplSet(SyncObjConsumer):
    def __init__(self):
        super(ReplSet, self).__init__()
        self.__data = set()

    @replicated
    def reset(self, newData):
        assert isinstance(newData, set)
        self.__data = newData

    @replicated
    def add(self, item):
        self.__data.add(item)

    @replicated
    def remove(self, item):
        self.__data.remove(item)

    @replicated
    def discard(self, item):
        self.__data.discard(item)

    @replicated
    def pop(self):
        self.__data.pop()

    @replicated
    def clear(self):
        self.__data.clear()

    @replicated
    def update(self, other):
        self.__data.update(other)

    def rawData(self):
        return self.__data

    def __len__(self):
        return len(self.__data)

    def __contains__(self, item):
        return item in self.__data
