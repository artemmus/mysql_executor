"""
.. module:: async_cursor
.. moduleauthor:: Artem Mustafa <artemmus@yahoo.com>
"""

import mysql.connector
import mysql.connector.cursor
from mysql.connector import errorcode
import asyncio
from functools import partial


__all__ = ['AsyncMySQLCursor']


class AsyncMySQLCursor(mysql.connector.cursor.MySQLCursor):
    def __init__(self,
                 base_cursor: mysql.connector.cursor.MySQLCursor,
                 executor,
                 *,
                 loop=None):
        super().__init__()
        self._cursor = base_cursor
        self._executor = executor
        self._loop = loop or asyncio.get_event_loop()

    @asyncio.coroutine
    def _run_in_executor(self, fn, *args, **kwargs):
        return (
            yield from self._loop.run_in_executor(
                self._executor, partial(fn, *args, **kwargs)
            )
        )

    @asyncio.coroutine
    def callproc(self, procname, args=()):
        """Coroutine. Calls a stored procedure with the given arguments

        The arguments will be set during this session, meaning
        they will be called like  _<procname>__arg<nr> where
        <nr> is an enumeration (+1) of the arguments.

        Coding Example:
          1) Defining the Stored Routine in MySQL:
          CREATE PROCEDURE multiply(IN pFac1 INT, IN pFac2 INT, OUT pProd INT)
          BEGIN
            SET pProd := pFac1 * pFac2;
          END

          2) Executing in Python:
          args = (5,5,0) # 0 is to hold pprod
          yield from cursor.callproc('multiply', args)
          print(cursor.fetchone())

        Does not return a value, but a result set will be
        available when the CALL-statement execute successfully.
        Raises exceptions when something is wrong.
        """
        yield from self._run_in_executor(self._cursor.callproc, procname, args)

    def close(self):
        """Close the cursor."""
        self._cursor.close()

    @asyncio.coroutine
    def execute(self, operation, params=(), multi=False):
        """Coroutine. Executes the given operation

        Executes the given operation substituting any markers with
        the given parameters.

        For example, getting all rows where id is 5:
          cursor.execute("SELECT * FROM t1 WHERE id = %s", (5,))

        The multi argument should be set to True when executing multiple
        statements in one operation. If not set and multiple results are
        found, an InterfaceError will be raised.

        If warnings where generated, and connection.get_warnings is True, then
        self._warnings will be a list containing these warnings.

        Returns an iterator when multi is True, otherwise None.
        """
        return (
            yield from self._run_in_executor(
                self._cursor.execute, operation, params, multi
            )
        )

    @asyncio.coroutine
    def executemany(self, operation, seqparams):
        """Coroutine. Execute the given operation multiple times

        The executemany() method will execute the operation iterating
        over the list of parameters in seq_params.

        Example: Inserting 3 new employees and their phone number

        data = [
            ('Jane','555-001'),
            ('Joe', '555-001'),
            ('John', '555-003')
            ]
        stmt = "INSERT INTO employees (name, phone) VALUES ('%s','%s')"
        cursor.executemany(stmt, data)

        INSERT statements are optimized by batching the data, that is
        using the MySQL multiple rows syntax.

        Results are discarded. If they are needed, consider looping over
        data using the execute() method.
        """
        yield from self._run_in_executor(self._cursor.executemany,
                                         operation, seqparams)

    @asyncio.coroutine
    def fetchone(self):
        """Coroutine. Returns next row of a query result set

        Returns a tuple or None.
        """
        return (yield from self._run_in_executor(self._cursor.fetchone))

    @asyncio.coroutine
    def fetchmany(self, size=1):
        """Coroutine. Returns the next set of rows of a query result,
        returning a list of tuples. When no more rows are available,
        it returns an empty list.

        The number of rows returned can be specified using the size argument,
        which defaults to one
        """
        return (yield from self._run_in_executor(self._cursor.fetchmany, size))

    @asyncio.coroutine
    def fetchall(self):
        """Coroutine. Returns all rows of a query result set

        Returns a list of tuples.
        """
        return (yield from self._run_in_executor(self._cursor.fetchall))

    @asyncio.coroutine
    def fetchwarnings(self):
        """Coroutine. Returns Warnings."""
        return (yield from self._run_in_executor(self._cursor.fetchwarnings))

    def __iter__(self):
        """
        Because there is no way to iterate in asyncio, restrict iterator
        """
        raise RuntimeError('AsyncMySQLCursor has not iterator')

    def reset(self):
        """Reset the cursor to default"""
        return self._cursor.reset()

    @property
    def description(self):
        """Returns description of columns in a result

        This property returns a list of tuples describing the columns in
        in a result set. A tuple is described as follows::

                (column_name,
                 type,
                 None,
                 None,
                 None,
                 None,
                 null_ok,
                 column_flags)  # Addition to PEP-249 specs

        Returns a list of tuples.
        """
        return self._cursor.description

    @property
    def rowcount(self):
        """Returns the number of rows produced or affected

        This property returns the number of rows produced by queries
        such as a SELECT, or affected rows when executing DML statements
        like INSERT or UPDATE.

        Note that for non-buffered cursors it is impossible to know the
        number of rows produced before having fetched them all. For those,
        the number of rows will be -1 right after execution, and
        incremented when fetching rows.

        Returns an integer.
        """
        return self._cursor.rowcount

    @property
    def lastrowid(self):
        """Returns the value generated for an AUTO_INCREMENT column

        Returns the value generated for an AUTO_INCREMENT column by
        the previous INSERT or UPDATE statement or None when there is
        no such value available.

        Returns a long value or None.
        """
        return self._cursor.lastrowid

    @property
    def column_names(self):
        """Returns column names

        This property returns the columns names as a tuple.

        Returns a tuple.
        """
        if not self._cursor.description:
            return ()
        return tuple([d[0] for d in self._cursor.description])

    @property
    def statement(self):
        """Returns the executed statement

        This property returns the executed statement. When multiple
        statements were executed, the current statement in the iterator
        will be returned.
        """
        return self._cursor.statement

    @property
    def with_rows(self):
        """Returns whether the cursor could have rows returned

        This property returns True when column descriptions are available
        and possibly also rows, which will need to be fetched.

        Returns True or False.
        """
        if not self._cursor.description:
            return False
        return True

    def __str__(self):
        fmt = "AsyncMySQLCursor: %s"
        if self._cursor._executed:
            executed = bytearray(self._cursor._executed).decode('utf-8')
            if len(executed) > 30:
                res = fmt % (executed[:30] + '..',)
            else:
                res = fmt % (executed,)
        else:
            res = fmt % '(Nothing executed yet)'
        return res
