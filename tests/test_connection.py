"""
.. module:: test_connection
.. moduleauthor:: Artem Mustafa <artemmus@yahoo.com>
"""

import unittest
import asyncio

from tests import asyncio_test
from tests.config import MYSQL_CONFIG
from mysql_executor import *


class TestAsyncConnection(unittest.TestCase):
    @asyncio_test
    def test_transaction(self, loop=None):
        """Testing asynchronous access with transaction"""

        pool = AsyncConnectionPool(loop=loop, **MYSQL_CONFIG)

        with (yield from pool) as cnx:
            yield from cnx.start_transaction()
            self.assertTrue(cnx.in_transaction)

            yield from cnx.commit()
            self.assertFalse(cnx.in_transaction)

            yield from cnx.start_transaction()
            self.assertTrue(cnx.in_transaction)

            yield from cnx.rollback()
            self.assertFalse(cnx.in_transaction)

        yield from pool.shutdown()

    @asyncio_test
    def test_disconnect(self, loop=None):
        pool = AsyncConnectionPool(size=1, loop=loop, **MYSQL_CONFIG)

        cnx = yield from pool.get()
        self.assertTrue((yield from cnx.is_connected()))

        cursor = yield from cnx.async_cursor()
        self.assertIsNotNone(cursor)

        # close connection
        yield from cnx.close()
        pool.release(cnx)

        # must be disconnected
        cnx2 = yield from pool.get()
        self.assertFalse((yield from cnx2.is_connected()))

        # the same as previous one
        self.assertIs(cnx, cnx2)

        cursor = yield from cnx.async_cursor()
        self.assertIsNotNone(cursor)
        # now must be connected
        self.assertTrue((yield from cnx2.is_connected()))

        # now test reconnection in the transaction
        yield from cnx2.close()
        yield from cnx2.start_transaction()
        self.assertTrue((yield from cnx2.is_connected()))

        yield from pool.shutdown()

    @asyncio_test
    def test_reconnect(self, loop=None):
        pool = AsyncConnectionPool(loop=loop, **MYSQL_CONFIG)

        cnx = yield from pool.get()
        self.assertTrue((yield from cnx.is_connected()))

        yield from cnx.close()
        self.assertFalse((yield from cnx.is_connected()))

        yield from cnx.reconnect()
        self.assertTrue((yield from cnx.is_connected()))

        yield from pool.shutdown()