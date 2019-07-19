import asyncio
import logging
import time


logger = logging.getLogger(__name__)


def cached(func):
    async def process(f, *args, **params):
        if asyncio.iscoroutinefunction(func):
            return await f(*args, **params)
        else:
            return f(*args, **params)

    async def helper(*args, **params):
        start = time.time()
        result = await process(func, *args, **params)
        elapsed = round(1000 * (time.time() - start))
        logger.info(f"{func.__name__} {elapsed}ms")
        return result

    return helper
