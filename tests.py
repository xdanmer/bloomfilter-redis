import asyncio
from datetime import datetime, timedelta

import aioredis
import pytest

from bloomfilter import BloomFilter, TimeSeriesBloomFilter


@pytest.fixture()
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(loop)
    request.addfinalizer(loop.close)
    return loop


async def create_redis(request):
    conn_pool = await aioredis.create_pool(('127.0.0.1', 6379), db=0, maxsize=5)
    async with conn_pool.get() as conn:
        conn.flushdb()

    request.addfinalizer(conn_pool.close)
    return conn_pool


async def create_single(request):
    conn_pool = await create_redis(request)
    single = BloomFilter(conn_pool=conn_pool,
                         bitvector_key='test_bloomfilter',
                         n=1024 * 8,
                         k=4)
    return single


async def create_time_series(request):
    conn_pool = await create_redis(request)
    timeseries = TimeSeriesBloomFilter(conn_pool=conn_pool,
                                       bitvector_key='test_timed_bloomfilter',
                                       capacity=1000,
                                       error_rate=0.01,
                                       time_resolution=timedelta(seconds=1),
                                       time_limit=timedelta(seconds=10))

    return timeseries


@pytest.mark.asyncio
async def test_add(request, event_loop):
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
async def test_delete(request, event_loop):
    f = await create_single(request)

    await f.add_async(['ten'])
    assert (await f.contains_async(['ten', 'five', 'two', 'eleven']))['ten']

    await f.delete_async('ten')
    assert not (await f.contains_async(['ten', 'five', 'two', 'eleven']))['ten']


@pytest.mark.asyncio
async def test_timeseries_add(request, event_loop):
    f = await create_time_series(request)

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
async def test_timeseries_delete(request, event_loop):
    f = await create_time_series(request)

    await f.add_async(['ten'])
    assert (await f.contains_async(['ten', 'five', 'two', 'eleven']))['ten']

    await f.delete_async('ten')
    assert not (await f.contains_async(['ten', 'five', 'two', 'eleven']))['ten']


@pytest.mark.asyncio
async def test_timeseries_delay(request, event_loop):
    f = await create_time_series(request)

    await f.add_async(['test_value'])
    start = datetime.now()
    # allow for 3ms delay in storing/timer resolution
    delay = timedelta(seconds=3)

    # make sure that the filter doesn't say that test_value is in the filter for too long
    while (await f.contains_async(['test_value']))['test_value']:
        assert datetime.now() < (start + timedelta(seconds=10) + delay)
    assert not (await f.contains_async(['test_value']))['test_value']
