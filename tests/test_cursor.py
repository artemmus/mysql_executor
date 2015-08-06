"""
.. module:: test_cursor
.. moduleauthor:: Artem Mustafa <artemmus@yahoo.com>
"""

import unittest
import asyncio

from tests import AsyncioTestConnectable


class TestAsyncCursor(unittest.TestCase, AsyncioTestConnectable):
    def test_async_fetchone(self):
        cursor = yield from self.cnx.async_cursor(named_tuple=True)
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
        self.assertEqual(row_ind, 2)

    def test_async_fetchmany(self):
        cursor = yield from self.cnx.async_cursor(named_tuple=True)
        self.assertIsNotNone(cursor)

        yield from cursor.execute('SELECT 1 AS first, 2 AS second '
                                  'UNION '
                                  'SELECT 2 AS first, 3 AS second ')
        row_ind = 1
        while True:
            row = yield from cursor.fetchmany(1)
            if not row:
                break

            self.assertEqual(row.first, row_ind)
            self.assertEqual(row.second, row_ind + 1)
            row_ind += 1
        self.assertEqual(row_ind, 2)

        yield from cursor.execute('SELECT 1 AS first, 2 AS second '
                                  'UNION '
                                  'SELECT 2 AS first, 3 AS second ')
        rows = yield from cursor.fetchmany(2)
        self.assertIsInstance(rows, list)
        self.assertEqual(len(rows), 2)

        for row_ind, row in enumerate(rows, start=1):
            self.assertEqual(row[0], row_ind)
            self.assertEqual(row[1], row_ind + 1)

    def test_async_fetchall(self):
        cursor = yield from self.cnx.async_cursor()
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

    def test_async_executemany(self):
        cursor = yield from self.cnx.async_cursor()
        self.assertIsNotNone(cursor)

        yield from cursor.execute('CREATE TEMPORARY TABLE testexecmany '
                                  '(id INT PRIMARY KEY)')

        yield from cursor.executemany('INSERT INTO testexecmany '
                                      '(id) VALUES (%s)',
                                      [(1), (2)])

        yield from cursor.execute('SELECT id FROM testexecmany')
        rows = yield from cursor.fetchall()
        self.assertIsInstance(rows, list)
        self.assertEqual(len(rows), 2)

        for row_ind, row in enumerate(rows, start=1):
            self.assertEqual(row[0], row_ind)
