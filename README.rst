=========================
bloomfilter-redis asyncio
=========================

Standard & time series bloom filters, backed by Redis bit vectors.

This is Python 3 asyncio implementation.

Performant Python 2 C-based extension that uses hiredis at https://github.com/seomoz/pyreBloom


Overview
========

This is the little bloom filter we're using to filter unique views using redis.

It doesn't do anything special, but I didn't find any small and dependency-free bloom
filter written in Python that use Redis as their backend.

Time Series
========
If you're tracking users over time, and you want to answer the question "have we seen
this guy in the past 2 minutes", this is exactly right for you. For high-throughput
applications this is very space-effective. The total memory footprint is known before-
hand, and is based on the amount of history you want to save and the resolution.

You might track users in the past 2 minutes with a 10-second resolution using 12 bloom
filters. User hits are logged into the most recent bloom filter, and checking if you have
seen a user in the past 2 minutes will just go back through those 12 filters.

The finest resolutions possible are around 1ms. If you're pushing it to this limit you'll
have to take care of a bunch of things: Storing to and retrieving from Redis takes some
time. Timestamps aren't all that exact, especially when running on a virtual machine. If
you're using multiple machines, their clocks have to be perfectly in sync.
