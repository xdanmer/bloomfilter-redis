import asyncio
import random
from datetime import datetime, timedelta

import aioredis

from bloomfilterredis.bloomfilter import TimeSeriesBloomFilter

test_amount = 1000 * 10
capacity = 24000000
batch = 500

host = 'localhost'
port = 6379
db = 0


async def main():
    conn_pool = await aioredis.create_pool((host, port), db=db, maxsize=20)
    async with conn_pool.get() as conn:
        conn.flushdb()

    bloom = TimeSeriesBloomFilter(bitvector_key='bloom',
                                  capacity=capacity,
                                  error_rate=0.01,
                                  time_limit=timedelta(days=3),
                                  time_resolution=timedelta(hours=36),
                                  conn_pool=conn_pool)


    print("filling bloom filter of %.2fkB size with %.1fk values" % \
          (bloom._bits_count / 1024.0 / 8, test_amount / 1000.0))

    # create a reference dict so that we can check for false positives
    ref = {}
    for x in range(test_amount):
        ref['%.8f' % random.random()] = True

    # add values to filter
    keys = list(ref.keys())

    start = datetime.now()
    futures = []
    for i in range(len(keys) // batch):
        to_add = [k for k in keys[i:i + batch]]
        future = bloom.add_async(to_add)
        futures.append(future)
    await asyncio.gather(*futures)

    # calculate results
    duration = datetime.now() - start
    duration = duration.seconds + duration.microseconds / 1000000.0
    per_second = test_amount / duration
    print("adding %i values took %.5fs (%i values/sec, %.2f us/value)" % \
          (test_amount, duration, per_second, 1000000.0 / per_second))

    # try random values and see how many false positives we'll get
    false_positives = 0
    correct_responses = 0
    start = datetime.now()
    while correct_responses < test_amount:
        futures = []
        for i in range(10):
            vals = ['%.8f' % random.random() for i in range(batch)]
            futures.append(bloom.contains_async(vals))

        print("start gather: ", datetime.now())

        results = await asyncio.gather(*futures)

        duration = datetime.now() - start
        print("gather completed. duration: ", duration)

        for res in results:
            for key, is_contained in res.items():
                if is_contained and (key not in ref):
                    false_positives += 1
                else:
                    correct_responses += 1

        duration = datetime.now() - start
        print("counting completed. duration: ", duration)

    duration = datetime.now() - start
    print("checking duration:", duration)
    print("correct: %s / false: %s -> %.4f%% false positives" % \
          (correct_responses, false_positives, 100 * false_positives / float(correct_responses)))

    async with conn_pool.get() as conn:
        conn.flushdb()


loop = asyncio.get_event_loop_policy().new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(main())
loop.close()
