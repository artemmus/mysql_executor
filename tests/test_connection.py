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