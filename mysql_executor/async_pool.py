"""
.. module:: async_pool
.. moduleauthor:: Artem Mustafa <artemmus@yahoo.com>
"""

import asyncio
from asyncio import Future, Event
from collections import deque
from concurrent.futures import TimeoutError

from .async_connection import AsyncMySQLConnection
from .utils import ContextManager

__all__ = ['AsyncConnectionPool']


class AsyncConnectionPool:
    """Object manages asynchronous connections.

    :param int size: size (number of connection) of the pool.
    :param float queue_timeout: time out when client is waiting connection
        from pool
    :param loop: event loop, if not passed then default will be used
    :param config: MySql connection config see
        `doc. <http://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html>`_
    :raise ValueError: if the `size` is inappropriate
    """
    def __init__(self, size=1, queue_timeout=15.0, *, loop=None, **config):
        assert size > 0, 'DBPool.size must be greater than 0'
        if size < 1:
            raise ValueError('DBPool.size is less than 1, '
                             'connections won"t be established')
        self._pool = set()
        self._busy_items = set()
        self._size = size
        self._pending_futures = deque()
        self._queue_timeout = queue_timeout
        self._loop = loop or asyncio.get_event_loop()
        self.config = config

        self._shutdown_event = Event(loop=self._loop)
        self._shutdown_event.set()

    @property
    def queue_timeout(self):
        """Number of seconds to wait a connection from the pool,
        before TimeoutError occurred

        :rtype: float
        """
        return self._queue_timeout

    @queue_timeout.setter
    def queue_timeout(self, value):
        """Sets a timeout for :attr:`queue_timeout`

        :param float value: number of seconds
        """
        if not isinstance(value, (float, int)):
            raise ValueError('Float or integer type expected')
        self._queue_timeout = value

    @property
    def size(self):
        """Size of pool

        :rtype: int
        """
        return self._size

    def __len__(self):
        """Number of allocated pool's slots

        :rtype: int
        """
        return len(self._pool)

    @property
    def free_count(self):
        """Number of free pool's slots

        :rtype: int
        """
        return self.size - len(self._busy_items)

    @asyncio.coroutine
    def get(self):
        """Coroutine. Returns an opened connection from pool.
        If coroutine invoked when all connections have been issued, then
        caller will blocked until some connection will be released.

        Also, the class provides context manager for getting connection
        and automatically freeing it. Example:
        >>> with (yield from pool) as cnx:
        >>>     ...

        :rtype: AsyncMySQLConnection
        :raise: concurrent.futures.TimeoutError()
        """
        cnx = None

        yield from self._shutdown_event.wait()

        for free_client in self._pool - self._busy_items:
            cnx = free_client
            self._busy_items.add(cnx)
            break
        else:
            if len(self) < self.size:
                cnx = AsyncMySQLConnection(loop=self._loop)
                self._pool.add(cnx)
                self._busy_items.add(cnx)

                try:
                    yield from cnx.connect(**self.config)
                except:
                    self._pool.remove(cnx)
                    self._busy_items.remove(cnx)
                    raise

        if not cnx:
            queue_future = Future(loop=self._loop)
            self._pending_futures.append(queue_future)
            try:
                cnx = yield from asyncio.wait_for(queue_future,
                                                  self.queue_timeout,
                                                  loop=self._loop)
                self._busy_items.add(cnx)
            except TimeoutError:
                raise TimeoutError('Database pool is busy')
            finally:
                try:
                    self._pending_futures.remove(queue_future)
                except ValueError:
                    pass

        return cnx

    def release(self, connection):
        """Frees connection. After that the connection can be issued
        by :func:`get`.

        :param AsyncMySQLConnection connection: a connection received
            from :func:`get`
        """
        if len(self._pending_futures):
            f = self._pending_futures.popleft()
            f.set_result(connection)
        else:
            self._busy_items.remove(connection)

    @asyncio.coroutine
    def shutdown(self):
        """Coroutine. Closes all connections and purge queue of a waiting
        for connection.
        """
        self._shutdown_event.clear()
        try:
            for cnx in self._pool:
                yield from cnx.disconnect()

            for f in self._pending_futures:
                f.cancel()

            self._pending_futures.clear()
            self._pool = set()
            self._busy_items = set()
        finally:
            self._shutdown_event.set()

    def __enter__(self):
        raise RuntimeError(
            '"yield from" should be used as context manager expression')

    def __exit__(self, *args):
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass

    @asyncio.coroutine
    def __iter__(self):
        cnx = yield from self.get()
        return ContextManager(self, cnx)