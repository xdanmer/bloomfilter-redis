import asyncio
import math
import time
from datetime import datetime, timedelta
from typing import List, Dict, Iterable

# For hash functions see http://www.partow.net/programming/hashfunctions/index.html
# Author Arash Partow, CPL http://www.opensource.org/licenses/cpl1.0.php
from aioredis import RedisPool


def FNVHash(key):
    fnv_prime = 0x811C9DC5
    hash = 0
    for i in range(len(key)):
        hash *= fnv_prime
        hash ^= ord(key[i])
    return hash


def APHash(key):
    hash = 0xAAAAAAAA
    for i in range(len(key)):
        if ((i & 1) == 0):
            hash ^= ((hash << 7) ^ ord(key[i]) * (hash >> 3))
        else:
            hash ^= (~((hash << 11) + ord(key[i]) ^ (hash >> 5)))
    return hash


class TimeSeriesBloomFilter(object):
    # todo: make it more clear how all this works
    # todo: create a helper function that calculates the total amount of memory stored

    def __init__(self,
                 bitvector_key: str,
                 capacity: int,
                 error_rate: float,
                 time_resolution: timedelta,
                 time_limit: timedelta,
                 conn_pool=None):
        self._time_resolution = time_resolution
        self._time_limit = time_limit
        parts = time_limit // time_resolution
        capacity = capacity // parts
        self._time_limit_seconds = self._time_limit.days * 86400 + self._time_limit.seconds
        self._bitvector_key = bitvector_key
        self._bits_count = int(-(capacity * math.log(error_rate)) / (math.log(2) * math.log(2)))
        self._hashes_count = math.ceil(self._bits_count / capacity * math.log(2))
        self._conn_pool = conn_pool

    def _most_current_filters(self, within, now):
        resolution_microseconds = (self._time_resolution.days * 86400 + self._time_resolution.seconds) * 1e6 + \
                                  self._time_resolution.microseconds

        within_microseconds = (within.days * 86400 + within.seconds) * 1e6 + within.microseconds

        # how many bloom filters will we need to iterate for this?
        num_filters = int(math.ceil(within_microseconds / resolution_microseconds))

        # figure out what the passed timestamp really is
        current_microtimestamp = time.mktime(now.timetuple()) * 1e6 + now.microsecond

        # get a datetime object of the 'current' filter
        block = resolution_microseconds * math.floor(current_microtimestamp / resolution_microseconds)
        block_now = datetime.fromtimestamp(block / 1e6)

        for x in range(num_filters):
            filter_date = block_now - x * self._time_resolution
            filter_bitvector_key = '%s|%s' % (self._bitvector_key, filter_date.isoformat())
            yield BloomFilter(self._conn_pool, filter_bitvector_key, self._bits_count, self._hashes_count)

    async def add_async(self, keys: Iterable[str], **kwargs):
        within = kwargs.get('within', self._time_resolution)
        now = kwargs.get('now', datetime.now())

        futures = []
        # add to the current bloom filter
        for bloom_filter in self._most_current_filters(within=within, now=now):
            # we'll expire the bloom filter we're setting to after 'limit' + 1 seconds
            future = bloom_filter.add_async(keys, timeout=self._time_limit_seconds + 1)
            futures.append(future)

        await asyncio.gather(*futures)

    async def delete_async(self, key: str, **kwargs):
        within = kwargs.get('within', self._time_limit)
        now = kwargs.get('now', datetime.now())

        # delete from the time series bloomfilters
        for bloom_filter in self._most_current_filters(within=within, now=now):
            # in case of creating new filter when deleting, so check first
            if (await bloom_filter.contains_async([key]))[key]:
                await bloom_filter.delete_async(key)

    async def contains_async(self, keys: List[str], **kwargs) -> Dict[str, bool]:
        # checks if this time series bloom filter has
        # contained an element within the last x minutes
        within = kwargs.get('within', self._time_limit)
        now = kwargs.get('now', datetime.now())

        futures = []
        filters = list(self._most_current_filters(within=within, now=now))
        for bloom_filter in filters:
            future = bloom_filter.contains_async(keys)
            futures.append(future)

        results = await asyncio.gather(*futures)

        res = {key: any([results[i][key] for i in range(len(filters))]) for key in keys}
        return res


class BloomFilter(object):
    def __init__(self, conn_pool: RedisPool, bitvector_key: str, n: int, k: int):
        # create a bloom filter based on a redis connection, a bitvector_key (name) for it
        # and the settings n & k, which dictate how effective it will be
        # - n is the amount of bits it will use, I have had success with 85001024 (500kiB)
        #   for 100k values. If you have fewer, you can get away with using fewer bits.
        #   in general, the more bits, the fewer false positives
        # - k is the number of hash derivations it uses, too many will fill up the filter
        #   too quickly, not enough will lead to many false positives
        self._conn_pool = conn_pool
        self._bitvector_key = bitvector_key
        self._n = n
        self._k = k

    async def contains_async(self, keys: List[str]) -> Dict[str, bool]:
        async with self._conn_pool.get() as conn:
            pipe = conn.pipeline()
            for key in keys:
                for hashed_offset in self._calculate_offsets(key):
                    pipe.getbit(self._bitvector_key, hashed_offset)

            results = await pipe.execute()
            res = {keys[i]: all([x == 1 for x in results[i * self._k:i * self._k + self._k]]) for i in range(len(keys))}
            return res

    async def add_async(self, keys: Iterable[str], set_value=1, transaction=False, timeout=None):
        # set bits for every hash to 1
        # sometimes we can use pipelines here instead of MULTI,
        # which makes it a bit faster
        async with self._conn_pool.get() as conn:
            pipe = conn.multi_exec() if transaction else conn.pipeline()
            for key in keys:
                for hashed_offset in self._calculate_offsets(key):
                    pipe.setbit(self._bitvector_key, hashed_offset, set_value)

            if timeout is not None:
                pipe.expire(self._bitvector_key, timeout)

            await pipe.execute()

    async def delete_async(self, key: str):
        # delete is just an add with value 0
        # make sure the pipeline gets wrapped in MULTI/EXEC, so
        # that a deleted element is either fully deleted or not
        # at all, in case someone is checking 'contains' while
        # an element is being deleted
        await self.add_async([key], set_value=0, transaction=True)

    def _calculate_offsets(self, key):
        # we're using only two hash functions with different settings, as described
        # by Kirsch & Mitzenmacher: https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
        hash_1 = FNVHash(key)
        hash_2 = APHash(key)

        for i in range(self._k):
            yield (hash_1 + i * hash_2) % self._n
