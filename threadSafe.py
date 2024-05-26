import threading


class ThreadSafeBase:
    def __init__(self):
        self._lock = threading.RLock()

    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()


class TSList(ThreadSafeBase):
    def __init__(self):
        super().__init__()
        self._list = []

    def append(self, item):
        with self._lock:
            self._list.append(item)

    def extend(self, items):
        with self._lock:
            self._list.extend(items)

    def insert(self, index, item):
        with self._lock:
            self._list.insert(index, item)

    def remove(self, item):
        with self._lock:
            self._list.remove(item)

    def pop(self, index=-1):
        with self._lock:
            return self._list.pop(index)

    def clear(self):
        with self._lock:
            self._list.clear()

    def index(self, item):
        with self._lock:
            return self._list.index(item)

    def count(self, item):
        with self._lock:
            return self._list.count(item)

    def sort(self, *args, **kwargs):
        with self._lock:
            self._list.sort(*args, **kwargs)

    def reverse(self):
        with self._lock:
            self._list.reverse()

    def __getitem__(self, index):
        with self._lock:
            return self._list[index]

    def __setitem__(self, index, value):
        with self._lock:
            self._list[index] = value

    def __delitem__(self, index):
        with self._lock:
            del self._list[index]

    def __len__(self):
        with self._lock:
            return len(self._list)

    def __iter__(self):
        with self._lock:
            return iter(self._list.copy())

    def __str__(self):
        with self._lock:
            return str(self._list)


class TSDict(ThreadSafeBase):
    def __init__(self):
        super().__init__()
        self._dict = {}

    def __setitem__(self, key, value):
        with self._lock:
            self._dict[key] = value

    def __getitem__(self, key):
        with self._lock:
            return self._dict[key]

    def __delitem__(self, key):
        with self._lock:
            del self._dict[key]

    def get(self, key, default=None):
        with self._lock:
            return self._dict.get(key, default)

    def pop(self, key, default=None):
        with self._lock:
            return self._dict.pop(key, default)

    def clear(self):
        with self._lock:
            self._dict.clear()

    def keys(self):
        with self._lock:
            return list(self._dict.keys())

    def values(self):
        with self._lock:
            return list(self._dict.values())

    def items(self):
        with self._lock:
            return list(self._dict.items())

    def __len__(self):
        with self._lock:
            return len(self._dict)

    def __iter__(self):
        with self._lock:
            return iter(self._dict.copy())

    def __str__(self):
        with self._lock:
            return str(self._dict)
