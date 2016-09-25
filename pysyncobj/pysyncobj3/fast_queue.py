#
#  WARNING: this is generated file, use generate.sh to update it.
#
import queue
from collections import deque
import threading

# According to benchmarks, standard Queue is slow.
# Using FastQueue improves overall performance by ~15%
class FastQueue(object):
    def __init__(self, maxSize):
        self.__queue = deque()
        self.__lock = threading.Lock()
        self.__maxSize = maxSize

    def put_nowait(self, value):
        with self.__lock:
            if len(self.__queue) > self.__maxSize:
                raise queue.Full()
            self.__queue.append(value)

    def get_nowait(self):
        with self.__lock:
            if len(self.__queue) == 0:
                raise queue.Empty()
            return self.__queue.popleft()
