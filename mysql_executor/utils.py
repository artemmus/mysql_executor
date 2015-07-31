"""
.. module:: async_utils
.. moduleauthor:: Artem Mustafa <artemmus@yahoo.com>
"""

from mysql.connector import errors
from inspect import isgeneratorfunction
from asyncio import iscoroutine
from functools import wraps
import logging

CONNECT_ATTEMPTS = 2

log = logging.getLogger('mysql_executor')


def async_reconnectable(method):
    @wraps(method)
    def outer(self, *args, **kwargs):
        for attempts in range(CONNECT_ATTEMPTS):
            try:
                if isgeneratorfunction(method) or iscoroutine(method):
                    return (yield from method(self, *args, **kwargs))
                else:
                    return method(self, *args, **kwargs)
            except (errors.InterfaceError, errors.OperationalError) as err:
                if isinstance(err, errors.OperationalError) and \
                   err.errno != -1:
                    raise
                if attempts + 1 >= CONNECT_ATTEMPTS:
                    raise
                log.warning(
                    'Can not %s. %r. Try reconnect #%d.',
                    method.__name__, err, attempts + 1
                )
                yield from self.reconnect()
    return outer


class ContextManager:
    def __init__(self, pool, cnx):
        self._pool = pool
        self._cnx = cnx

    def __enter__(self):
        return self._cnx

    def __exit__(self, *args):
        self._pool.release(self._cnx)
        self._pool = None
        self._cnx = None