import asyncio


def asyncio_test(f):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

        coro = asyncio.coroutine(f)
        kwargs['loop'] = loop
        future = coro(*args, **kwargs)

        loop.run_until_complete(future)
        loop.close()
    return wrapper