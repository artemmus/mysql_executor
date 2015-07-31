"""
.. moduleauthor:: Artem Mustafa <artemmus@yahoo.com>
"""

import unittest
import asyncio
import mysql.connector

from tests import asyncio_test
from tests.config import MYSQL_CONFIG
from mysql_executor import *


class TestUsage(unittest.TestCase):
    def test_execute_sync(self):
        """Just check our environment"""

        cnx = mysql.connector.connect(**MYSQL_CONFIG)

        cur = cnx.cursor()
        cur.execute('SELECT 1')

        res = cur.fetchone()
        self.assertIsNotNone(res)
        self.assertEqual(res[0], 1)

        cnx.close()

    @asyncio_test
    def test_async_concurrent(self, loop=None):
        """Testing speed of asynchronous concurrent access"""

        concurrency = 10
        timeout = 1.0

        pool = AsyncConnectionPool(size=concurrency, loop=loop, **MYSQL_CONFIG)

        # getting connections
        cs = [asyncio.async(pool.get(), loop=loop) for _ in range(concurrency)]
        yield from asyncio.wait(cs, loop=loop, timeout=timeout)

        # getting cursors
        curs = [asyncio.async(cnx.result().async_cursor(), loop=loop)
                for cnx in cs]
        yield from asyncio.wait(curs, loop=loop, timeout=timeout)

        # executing queries
        stmts = [asyncio.async(cur.result().execute('SELECT 1'), loop=loop)
                 for cur in curs]
        yield from asyncio.wait(stmts, loop=loop, timeout=timeout)

        # fetching results
        results = [asyncio.async(cur.result().fetchone(), loop=loop)
                   for cur in curs]
        yield from asyncio.wait(results, loop=loop, timeout=timeout)

        for res_future in results:
            self.assertIsInstance(res_future.result(), tuple)
            self.assertEqual(res_future.result()[0], 1)

        # release connections
        for cnx in cs:
            pool.release(cnx.result())
        self.assertEqual(pool.free_count, concurrency)

        yield from pool.shutdown()