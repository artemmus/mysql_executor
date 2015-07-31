"""
.. module:: test_pool
.. moduleauthor:: Artem Mustafa <artemmus@yahoo.com>
"""

import unittest
import asyncio
from time import time

from tests import asyncio_test
from tests.config import MYSQL_CONFIG
from mysql_executor import *


class TestAsyncPool(unittest.TestCase):
    @asyncio_test
    def test_pool_get_release(self, loop=None):
        size = 3

        pool = AsyncConnectionPool(size=size, loop=loop, **MYSQL_CONFIG)
        self.assertEqual(pool.size, size)

        conn = []
        for i in reversed(range(size)):
            cnx = yield from pool.get()
            conn.append(cnx)
            self.assertEqual(pool.free_count, i)
            self.assertTrue((yield from cnx.is_connected()))

        for i, cnx in enumerate(conn, start=1):
            pool.release(cnx)
            self.assertEqual(pool.free_count, i)
            
        yield from pool.shutdown()
        self.assertEqual(pool.free_count, size)

    @asyncio_test
    def test_pool_queuing(self, loop=None):
        pool = AsyncConnectionPool(loop=loop, **MYSQL_CONFIG)

        cnx = yield from pool.get()
        loop.call_later(1.5, lambda: pool.release(cnx))

        start_wait = time()
        cnx2 = yield from pool.get()
        waited = time() - start_wait

        self.assertGreaterEqual(waited, 1.5)
        self.assertLess(waited, 1.55)
        self.assertIs(cnx, cnx2)

        yield from pool.shutdown()

    @asyncio_test
    def test_context_manager(self, loop=None):
        """Testing asynchronous access with context manager"""

        pool = AsyncConnectionPool(loop=loop, **MYSQL_CONFIG)

        with (yield from pool) as cnx:
            cursor = yield from cnx.async_cursor(named_tuple=True)
            self.assertIsNotNone(cursor)

            yield from cursor.execute('SELECT 1 AS first, 2 AS second')

            row = yield from cursor.fetchone()
            self.assertEqual(row.first, 1)
            self.assertEqual(row.second, 2)

        yield from pool.shutdown()
