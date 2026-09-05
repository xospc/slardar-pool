from abc import ABC, abstractmethod
from asyncio import shield, Future, AbstractEventLoop, CancelledError
from collections import deque
from typing import TypeVar, Generic

Connection = TypeVar("Connection")


class Pool(ABC, Generic[Connection]):
    def __init__(self, loop: AbstractEventLoop, max_size: int = 10):
        self._loop = loop
        self.max_size = max_size

        self._availables: deque[Connection] = deque()
        self._waitings: deque[Future[None]] = deque()

        self._creating_count = 0
        self._using_count = 0

    @abstractmethod
    async def _create_connection_impl(self) -> Connection:
        raise NotImplementedError

    @abstractmethod
    async def _close_connection_impl(self, conn: Connection) -> None:
        raise NotImplementedError

    @property
    def _available_count(self) -> int:
        return len(self._availables)

    @property
    def _waiting_count(self) -> int:
        return len(self._waitings)

    async def _create_connection_inner(self) -> None:
        try:
            self._creating_count += 1
            conn = await self._create_connection_impl()
            self._availables.append(conn)
        finally:
            self._creating_count -= 1

    async def _create_connection(self) -> Connection:
        await shield(self._create_connection_inner())

        if self._availables:
            return self._availables.popleft()
        else:
            raise ValueError("availables is empty")

    async def acquire(self) -> Connection:
        if not self._availables:
            if self._creating_count + self._using_count < self.max_size:
                conn = await self._create_connection()
            else:
                fut: Future[None] = self._loop.create_future()
                self._waitings.append(fut)

                try:
                    await fut
                except CancelledError:
                    self._waitings.popleft()
                    raise

                if self._availables:
                    conn = self._availables.popleft()
                else:
                    conn = await self._create_connection()
        else:
            conn = self._availables.popleft()

        self._using_count += 1

        return conn

    async def release(self, conn: Connection) -> None:
        self._using_count -= 1
        self._availables.append(conn)

        if self._waitings:
            fut = self._waitings.popleft()
            fut.set_result(None)

    async def drop(self, conn: Connection) -> None:
        self._using_count -= 1
        if self._waitings:
            fut = self._waitings.popleft()
            fut.set_result(None)

        await self._close_connection_impl(conn)
