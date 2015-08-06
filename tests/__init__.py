import asyncio

from mysql_executor import AsyncConnectionPool
from tests.config import MYSQL_CONFIG


def asyncio_test(f):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

        coro = asyncio.coroutine(f)
        kwargs['loop'] = loop
        future = coro(*args, **kwargs)

        loop.run_until_complete(future)
        loop.close()
    return wrapper


class AsyncioTestConnectable:
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

        self.pool = AsyncConnectionPool(loop=self.loop, **MYSQL_CONFIG)

        self.loop.run_until_complete(self.get_connection_coro())

    def tearDown(self):
        self.loop.run_until_complete(self.close_connection_coro())

        self.loop.close()

    @asyncio.coroutine
    def get_connection_coro(self):
        self.cnx = yield from self.pool.get()

    @asyncio.coroutine
    def close_connection_coro(self):
        yield from self.pool.shutdown()