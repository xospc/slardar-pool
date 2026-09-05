from asyncio import (
    get_running_loop, create_task, wait_for, sleep,
    AbstractEventLoop,
)
from typing import Optional
from unittest import IsolatedAsyncioTestCase

from slardar_pool.pool import Pool


class FakeConnection:
    pass


class FakePool(Pool[FakeConnection]):
    def __init__(
        self,
        loop: AbstractEventLoop,
        max_size: int = 10,
        delay: Optional[float] = None,
    ):
        super().__init__(loop=loop, max_size=max_size)

        self._delay = delay

    async def _create_connection_impl(self) -> FakeConnection:
        if self._delay is not None:
            await sleep(self._delay)

        return FakeConnection()

    async def _close_connection_impl(self, conn: FakeConnection) -> None:
        pass


class TestPool(IsolatedAsyncioTestCase):
    async def test_release(self) -> None:
        loop = get_running_loop()
        pool = FakePool(loop, max_size=3)

        rs = [
            await pool.acquire()
            for _ in range(3)
        ]
        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 3)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

        for i in rs:
            await pool.release(i)

        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 0)
        self.assertEqual(pool._available_count, 3)
        self.assertEqual(pool._waiting_count, 0)

    async def test_drop(self) -> None:
        loop = get_running_loop()
        pool = FakePool(loop, max_size=3)

        rs = [
            await pool.acquire()
            for _ in range(3)
        ]
        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 3)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

        for i in rs:
            await pool.drop(i)

        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 0)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

    async def test_release_after_full(self) -> None:
        loop = get_running_loop()
        pool = FakePool(loop, max_size=3)

        rs = [
            await pool.acquire()
            for _ in range(3)
        ]
        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 3)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

        task = create_task(
            pool.acquire()
        )
        await sleep(0.1)

        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 3)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 1)

        for i in rs:
            await pool.release(i)
        await sleep(0.1)

        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 1)
        self.assertEqual(pool._available_count, 2)
        self.assertEqual(pool._waiting_count, 0)

        self.assertTrue(task.done())

    async def test_drop_after_full(self) -> None:
        loop = get_running_loop()
        pool = FakePool(loop, max_size=3)

        rs = [
            await pool.acquire()
            for _ in range(3)
        ]
        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 3)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

        task = create_task(
            pool.acquire()
        )
        await sleep(0.1)

        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 3)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 1)

        for i in rs:
            await pool.drop(i)
        await sleep(0.1)

        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 1)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

        self.assertTrue(task.done())

    async def test_drop_after_release(self) -> None:
        loop = get_running_loop()
        pool = FakePool(loop, max_size=3)

        # acquire and release for 3 times
        for _ in range(3):
            c1 = await pool.acquire()
            self.assertEqual(pool._creating_count, 0)
            self.assertEqual(pool._using_count, 1)
            self.assertEqual(pool._available_count, 0)
            self.assertEqual(pool._waiting_count, 0)

            await pool.release(c1)
            self.assertEqual(pool._creating_count, 0)
            self.assertEqual(pool._using_count, 0)
            self.assertEqual(pool._available_count, 1)
            self.assertEqual(pool._waiting_count, 0)

        # acquire and drop
        c2 = await pool.acquire()
        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 1)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

        await pool.drop(c2)
        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 0)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

    async def test_time_out_for_release(self) -> None:
        loop = get_running_loop()
        pool = FakePool(loop, max_size=3)

        for _ in range(3):
            await pool.acquire()

        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 3)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

        task = create_task(
            wait_for(
                pool.acquire(),
                0.3
            )
        )
        await sleep(0.1)

        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 3)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 1)

        await sleep(0.4)

        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 3)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

        with self.assertRaises(TimeoutError):
            await task

    async def test_time_out_for_connect(self) -> None:
        loop = get_running_loop()
        pool = FakePool(loop, max_size=3, delay=0.4)

        task = create_task(
            wait_for(
                pool.acquire(),
                0.3
            )
        )
        await sleep(0.1)

        self.assertEqual(pool._creating_count, 1)
        self.assertEqual(pool._using_count, 0)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

        await sleep(0.4)

        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 0)
        self.assertEqual(pool._available_count, 1)
        self.assertEqual(pool._waiting_count, 0)

        with self.assertRaises(TimeoutError):
            await task

    async def test_time_out_for_connect_after_full(self) -> None:
        loop = get_running_loop()
        pool = FakePool(loop, max_size=3)

        rs = [
            await pool.acquire()
            for _ in range(3)
        ]
        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 3)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

        pool._delay = 0.4
        task = create_task(
            wait_for(
                pool.acquire(),
                0.3
            )
        )
        await sleep(0.1)

        # wait for connection
        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 3)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 1)

        for i in rs:
            await pool.drop(i)

        await sleep(0.1)

        # connection is begin created
        self.assertEqual(pool._creating_count, 1)
        self.assertEqual(pool._using_count, 0)
        self.assertEqual(pool._available_count, 0)
        self.assertEqual(pool._waiting_count, 0)

        await sleep(0.4)

        # connection is created but not acquired
        # put in available
        self.assertEqual(pool._creating_count, 0)
        self.assertEqual(pool._using_count, 0)
        self.assertEqual(pool._available_count, 1)
        self.assertEqual(pool._waiting_count, 0)

        with self.assertRaises(TimeoutError):
            await task
