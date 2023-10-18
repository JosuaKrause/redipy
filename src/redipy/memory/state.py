import collections
from typing import overload

from redipy.api import RedisAPI


class State:
    def __init__(self, parent: 'State | None' = None) -> None:
        super().__init__()
        self._parent = parent
        self._vals: dict[str, str] = {}
        self._queues: dict[str, collections.deque[str]] = {}
        # TODO: to be replaced by something more efficient later
        self._zorder: dict[str, list[str]] = {}
        self._zscores: dict[str, dict[str, float]] = {}

    def apply(self, other: 'State') -> None:
        self._vals.update(other.raw_vals())
        self._queues.update(other.raw_queues())
        self._zorder.update(other.raw_zorder())
        self._zscores.update(other.raw_zscores())

    def raw_vals(self) -> dict[str, str]:
        return self._vals

    def raw_queues(
            self) -> dict[str, collections.deque[str]]:
        return self._queues

    def raw_zorder(self) -> dict[str, list[str]]:
        return self._zorder

    def raw_zscores(self) -> dict[str, dict[str, float]]:
        return self._zscores

    def set_value(self, key: str, value: str) -> None:
        self._vals[key] = value

    def get_value(self, key: str) -> str | None:
        res = self._vals.get(key)
        if res is None and self._parent is not None:
            return self._parent.get_value(key)
        return res

    def get_queue(self, key: str) -> collections.deque[str]:
        res = self._queues.get(key)
        if res is None:
            if self._parent is not None:
                res = collections.deque(self.get_queue(key))
            else:
                res = collections.deque()
            self._queues[key] = res
        return res

    def queue_len(self, key: str) -> int:
        res = self._queues.get(key)
        if res is None:
            if self._parent is not None:
                return self._parent.queue_len(key)
            return 0
        return len(res)

    def get_zorder(self, key: str) -> list[str]:
        res = self._zorder.get(key)
        if res is None:
            if self._parent is not None:
                res = list(self.get_zorder(key))
            else:
                res = []
            self._zorder[key] = res
        return res

    def zorder_len(self, key: str) -> int:
        res = self._zorder.get(key)
        if res is None:
            if self._parent is not None:
                return self._parent.zorder_len(key)
            return 0
        return len(res)

    def get_zscores(self, key: str) -> dict[str, float]:
        res = self._zscores.get(key)
        if res is None:
            if self._parent is not None:
                res = dict(self.get_zscores(key))
            else:
                res = {}
            self._zscores[key] = res
        return res

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}["
            f"vals={self._vals},"
            f"queues={dict(self._queues)},"
            f"zorder={dict(self._zorder)},"
            f"zscores={dict(self._zscores)}]")

    def __repr__(self) -> str:
        return self.__str__()


class Machine(RedisAPI):
    def __init__(self, state: State) -> None:
        super().__init__()
        self._state = state

    def get_state(self) -> State:
        return self._state

    def set(self, key: str, value: str) -> str:
        # TODO implement rest of arguments https://redis.io/commands/set/
        self._state.set_value(key, value)
        return "OK"

    def get(self, key: str) -> str | None:
        return self._state.get_value(key)

    def lpush(self, key: str, *values: str) -> int:
        queue = self._state.get_queue(key)
        queue.extendleft(values)
        return len(queue)

    def rpush(self, key: str, *values: str) -> int:
        queue = self._state.get_queue(key)
        queue.extend(values)
        return len(queue)

    @overload
    def lpop(
            self,
            key: str,
            count: None = None) -> str | None:
        ...

    @overload
    def lpop(  # pylint: disable=signature-differs
            self,
            key: str,
            count: int) -> list[str] | None:
        ...

    def lpop(
            self,
            key: str,
            count: int | None = None) -> str | list[str] | None:
        queue = self._state.get_queue(key)
        if not queue:
            return None
        if count is None:
            return queue.popleft()
        popc = count
        res = []
        while popc > 0 and queue:
            res.append(queue.popleft())
            popc -= 1
        return res if res else None

    @overload
    def rpop(
            self,
            key: str,
            count: None = None) -> str | None:
        ...

    @overload
    def rpop(  # pylint: disable=signature-differs
            self,
            key: str,
            count: int) -> list[str] | None:
        ...

    def rpop(
            self,
            key: str,
            count: int | None = None) -> str | list[str] | None:
        queue = self._state.get_queue(key)
        if not queue:
            return None
        if count is None:
            return queue.pop()
        popc = count
        res = []
        while popc > 0 and queue:
            res.append(queue.pop())
            popc -= 1
        return res if res else None

    def llen(self, key: str) -> int:
        return self._state.queue_len(key)

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        count = 0
        zscores = self._state.get_zscores(key)
        zorder = self._state.get_zorder(key)
        for name, score in mapping.items():
            if name not in zscores:
                zorder.append(name)
                count += 1
            zscores[name] = score
        zorder.sort(key=lambda k: (zscores[k], k))
        return count

    def zpop_max(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        zscores = self._state.get_zscores(key)
        zorder = self._state.get_zorder(key)
        res = []
        remain = 1 if count is None else count
        while remain > 0 and zorder:
            name = zorder.pop()
            score = zscores.pop(name)
            res.append((name, score))
            remain -= 1
        return res

    def zpop_min(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        zscores = self._state.get_zscores(key)
        zorder = self._state.get_zorder(key)
        res = []
        remain = 1 if count is None else count
        while remain > 0 and zorder:
            name = zorder.pop(0)
            score = zscores.pop(name)
            res.append((name, score))
            remain -= 1
        return res

    def zcard(self, key: str) -> int:
        return self._state.zorder_len(key)
