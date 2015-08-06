"""
.. module:: async_connection
.. moduleauthor:: Artem Mustafa <artemmus@yahoo.com>
"""

import mysql.connector
import mysql.connector.cursor
from mysql.connector.cursor import (
    CursorBase, MySQLCursor, MySQLCursorBuffered, MySQLCursorRaw,
    MySQLCursorBufferedRaw, MySQLCursorDict, MySQLCursorBufferedDict,
    MySQLCursorNamedTuple, MySQLCursorBufferedNamedTuple, MySQLCursorPrepared
)
from mysql.connector import errors
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from .utils import async_reconnectable
from .async_cursor import AsyncMySQLCursor


__all__ = ['AsyncMySQLConnection']


class AsyncMySQLConnection(mysql.connector.MySQLConnection):
    def __init__(self, loop=None):
        super().__init__()
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._loop = loop or asyncio.get_event_loop()

    @asyncio.coroutine
    def _run_in_executor(self, fn, *args, **kwargs):
        return (
            yield from self._loop.run_in_executor(
                self._executor, partial(fn, *args, **kwargs)
            )
        )

    @asyncio.coroutine
    def connect(self, **kwargs):
        """Coroutine. Connect to the MySQL server

        This method sets up the connection to the MySQL server. If no
        arguments are given, it will use the already configured or default
        values.
        """
        yield from self._run_in_executor(super().connect, **kwargs)

    @asyncio.coroutine
    def disconnect(self):
        """Coroutine. Disconnect from the MySQL server
        """
        yield from self._run_in_executor(super().disconnect)
    close = disconnect

    @asyncio.coroutine
    def reconnect(self, attempts=1, delay=0):
        """Coroutine. Attempt to reconnect to the MySQL server

        The argument attempts should be the number of times a reconnect
        is tried. The delay argument is the number of seconds to wait between
        each retry.

        You may want to set the number of attempts higher and use delay when
        you expect the MySQL server to be down for maintenance or when you
        expect the network to be temporary unavailable.

        Raises InterfaceError on errors.
        """
        for step in range(attempts):
            try:
                yield from self.disconnect()
                yield from self._run_in_executor(super().connect)
            except:
                if step + 1 >= attempts:
                    raise
                yield from asyncio.sleep(delay)
            else:
                break

    @asyncio.coroutine
    def is_connected(self):
        """Coroutine. Reports whether the connection to MySQL Server
        is available

        This method checks whether the connection to MySQL is available.
        It is similar to ping(), but unlike the ping()-method, either True
        or False is returned and no exception is raised.

        Returns True or False.
        """
        try:
            yield from self._run_in_executor(super().cmd_ping)
        except:
            return False  # This method does not raise
        return True

    @asyncio.coroutine
    @async_reconnectable
    def start_transaction(self,
                          consistent_snapshot=False,
                          isolation_level=None,
                          readonly=None):
        """Coroutine. Start a transaction

        This method explicitly starts a transaction sending the
        START TRANSACTION statement to the MySQL server. You can optionally
        set whether there should be a consistent snapshot, which
        isolation level you need or which access mode i.e. READ ONLY or
        READ WRITE.

        .. note:: This method tries to reconnect if connection is not available

        For example, to start a transaction with isolation level SERIALIZABLE,
        you would do the following:
            >>> cn = yield from mysql.connector.connect(..)
            >>> yield from cn.start_transaction(isolation_level='SERIALIZABLE')

        Raises ProgrammingError when a transaction is already in progress
        and when ValueError when isolation_level specifies an Unknown
        level.
        """
        yield from self._run_in_executor(
            super().start_transaction,
            consistent_snapshot, isolation_level, readonly
        )

    @asyncio.coroutine
    def commit(self):
        """Coroutine. Commit current transaction"""
        yield from self._run_in_executor(super().commit)

    @asyncio.coroutine
    def rollback(self):
        """Coroutine. Rollback current transaction"""
        yield from self._run_in_executor(super().rollback)

    @asyncio.coroutine
    @async_reconnectable
    def async_cursor(self, buffered=None, raw=None, prepared=None,
                     cursor_class=None, dictionary=None, named_tuple=None):
        """Coroutine. Instantiates and returns a cursor

        .. note:: This method tries to reconnect if connection is not available

        By default, MySQLCursor is returned. Depending on the options
        while connecting, a buffered and/or raw cursor is instantiated
        instead. Also depending upon the cursor options, rows can be
        returned as dictionary or named tuple.

        Dictionary and namedtuple based cursors are available with buffered
        output but not raw.

        It is possible to also give a custom cursor through the
        cursor_class parameter, but it needs to be a subclass of
        mysql.connector.cursor.CursorBase.

        Raises ProgrammingError when cursor_class is not a subclass of
        CursorBase. Raises ValueError when cursor is not available.

        Returns a cursor-object
        """
        if self._unread_result is True:
            raise errors.InternalError("Unread result found.")
        if not (yield from self.is_connected()):
            raise errors.OperationalError("MySQL Connection not available.")
        if cursor_class is not None:
            if not issubclass(cursor_class, CursorBase):
                raise errors.ProgrammingError(
                    "Cursor class needs be to subclass of cursor.CursorBase")
            return AsyncMySQLCursor(
                cursor_class(self),
                self._executor,
                loop=self._loop
            )

        buffered = buffered or self._buffered
        raw = raw or self._raw

        cursor_type = 0
        if buffered is True:
            cursor_type |= 1
        if raw is True:
            cursor_type |= 2
        if dictionary is True:
            cursor_type |= 4
        if named_tuple is True:
            cursor_type |= 8
        if prepared is True:
            cursor_type |= 16

        types = {
            0: MySQLCursor,  # 0
            1: MySQLCursorBuffered,
            2: MySQLCursorRaw,
            3: MySQLCursorBufferedRaw,
            4: MySQLCursorDict,
            5: MySQLCursorBufferedDict,
            8: MySQLCursorNamedTuple,
            9: MySQLCursorBufferedNamedTuple,
            16: MySQLCursorPrepared
        }
        try:
            return AsyncMySQLCursor(
                (types[cursor_type])(self),
                self._executor,
                loop=self._loop
            )
        except KeyError:
            args = ('buffered', 'raw', 'dictionary', 'named_tuple', 'prepared')
            raise ValueError('Cursor not available with given criteria: ' +
                             ', '.join([args[i] for i in range(5)
                                        if cursor_type & (1 << i) != 0]))

    def __del__(self):
        self._executor.shutdown(False)
