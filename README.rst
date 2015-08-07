Non-blocking version of mysql-connector-python for asyncio
==========================================================

Main Features
-------------

* Python 3.3+ compatible
* Inherited from `mysql-connector-python <http://dev.mysql.com/doc/connector-python/en/index.html>`_
* Based on asyncio
* Provides non-blocking access to MySQL

Install
-------
pip install git+https://github.com/artemmus/mysql_executor.git

Usage
-----
.. code-block:: python

    import asyncio
    from mysql_executor import *


    @asyncio.coroutine
    def example(**mysql_connect_params):
        pool = AsyncConnectionPool(size=1, **mysql_connect_params)

        with (yield from pool) as cnx:
            yield from cnx.start_transaction()

            cursor = yield from cnx.async_cursor(named_tuple=True)
            yield from cursor.execute('SELECT 1 AS first')

            row = yield from cursor.fetchone()
            assert row.first == 1

            yield from cnx.commit()

        yield from pool.shutdown()