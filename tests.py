import asyncio
from datetime import datetime, timedelta

import aioredis
import pytest

from bloomfilter import BloomFilter, TimeSeriesBloomFilter

loop = asyncio.get_event_loop_policy().new_event_loop()
asyncio.set_event_loop(loop)


async def create_redis():
    conn_pool = await aioredis.create_pool(('127.0.0.1', 6379), db=0, maxsize=5)
    async with conn_pool.get() as conn:
        conn.flushdb()

    return conn_pool


async def create_single(request):
    conn_pool = await create_redis()
    single = BloomFilter(conn_pool=conn_pool,
                         bitvector_key='test_bloomfilter',
                         n=1024 * 8,
                         k=4)

    request.addfinalizer(conn_pool.close)
    return single


async def create_time_series(request):
    conn_pool = await create_redis()
    timeseries = TimeSeriesBloomFilter(conn_pool=conn_pool,
                                       bitvector_key='test_timed_bloomfilter',
                                       capacity=1000,
                                       error_rate=0.01,
                                       time_resolution=timedelta(microseconds=1000),
                                       time_limit=timedelta(microseconds=10000))

    request.addfinalizer(conn_pool.close)
    return timeseries


@pytest.mark.asyncio
async def test_add(request):
    f = await create_single(request)

    await f.add_async(['three',
                       'four',
                       'five',
                       'six',
                       'seven',
                       'eight',
                       'nine',
                       'ten'])
    # test membership operations
    res = await f.contains_async(['ten', 'two', 'five', 'eleven'])

    assert res['ten']
    assert res['five']
    assert not res['eleven']
    assert not res['two']


@pytest.mark.asyncio
async def test_delete(request):
    f = await create_single(request)

    await f.add_async(['ten'])
    contained = await f.contains_async(['ten', 'five', 'two', 'eleven'])
    assert contained['ten']

    await f.delete_async('ten')
    contained = await f.contains_async(['ten', 'five', 'two', 'eleven'])
    assert not contained['ten']


@pytest.mark.asyncio
async def test_timeseries_add(request):
    f = await create_time_series(request)

    contained = await f.contains_async(['test_value'])
    assert not contained['test_value']

    await f.add_async(['test_value'])
    contained = await f.contains_async(['test_value'])
    assert contained['test_value']


@pytest.mark.asyncio
async def test_timeseries_delay(request):
    f = await create_time_series(request)

    await f.add_async(['test_value'])
    start = datetime.now()
    # allow for 3ms delay in storing/timer resolution
    delay = timedelta(seconds=3)

    # make sure that the filter doesn't say that test_value is in the filter for too long
    while (await f.contains_async(['test_value']))['test_value']:
        assert datetime.now() < (start + timedelta(seconds=10) + delay)
    assert not (await f.contains_async(['test_value']))['test_value']
