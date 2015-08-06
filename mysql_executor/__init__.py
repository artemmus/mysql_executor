"""
.. module:: mysql_executor
.. moduleauthor:: Artem Mustafa <artemmus@yahoo.com>

Non-blocking version of `mysql-python-connector
<http://dev.mysql.com/doc/connector-python/en/index.html>`_
"""

from .async_pool import AsyncConnectionPool
from .async_connection import AsyncMySQLConnection
from .async_cursor import AsyncMySQLCursor

__version__ = '0.2.0'


def version():
    return __version__