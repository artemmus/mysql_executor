"""
.. module:: test_cursor
.. moduleauthor:: Artem Mustafa <artemmus@yahoo.com>
"""

import unittest
import asyncio

from tests import asyncio_test
from tests.config import MYSQL_CONFIG
from mysql_executor import *


class TestAsyncCursor(unittest.TestCase):
    @asyncio_test
    def test_async_fetchone(self, loop=None):
        pool = AsyncConnectionPool(loop=loop, **MYSQL_CONFIG)

        cnx = yield from pool.get()
        self.assertTrue(cnx.is_connected())

        cursor = yield from cnx.async_cursor(named_tuple=True)
        self.assertIsNotNone(cursor)

        yield from cursor.execute('SELECT 1 AS first, 2 AS second '
                                  'UNION '
                                  'SELECT 2 AS first, 3 AS second ')
        row_ind = 1
        while True:
            row = yield from cursor.fetchone()
            if not row:
                break

            self.assertEqual(row.first, row_ind)
            self.assertEqual(row.second, row_ind + 1)
            row_ind += 1

        yield from pool.shutdown()

    @asyncio_test
    def test_async_fetchall(self, loop=None):
        pool = AsyncConnectionPool(loop=loop, **MYSQL_CONFIG)

        with (yield from pool) as cnx:
            cursor = yield from cnx.async_cursor()
            self.assertIsNotNone(cursor)

            yield from cursor.execute('SELECT 1 AS first, 2 AS second '
                                      'UNION '
                                      'SELECT 2 AS first, 3 AS second ')
            rows = yield from cursor.fetchall()
            self.assertIsInstance(rows, list)
            self.assertEqual(len(rows), 2)

            for row_ind, row in enumerate(rows, start=1):
                self.assertEqual(row[0], row_ind)
                self.assertEqual(row[1], row_ind + 1)

        yield from pool.shutdown()
