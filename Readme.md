# Petrify

High-durability persistence for log-structured datastores.


## Overview

Petrify provides continuous, incremental persistence to and point-in-time recovery from [Amazon S3](http://aws.amazon.com/s3/) for arbitrary log-structured datastores, including [Redis](http://redis.io), [CouchDB](http://couchdb.apache.org/), [FleetDB](http://fleetdb.org), and [Doozer](https://github.com/ha/doozer)-via-[Gorg](https://github.com/mmcgrana/gorg). Persisting state to S3 is desirable because of S3's superior durability, availability, and simplicity as compared to ephemeral or replicated file systems.

Petrify interfaces to datastores via append-only log files. Petrify periodically stores a snapshot of the datastore's log to S3 and more frequently posts log deltas to S3. The snapshots and deltas can then be used to recover the datastore's state to the point in time of the last posted log delta, or more generally to any point in time with the granularity of the delta interval.


## Usage

We will persist a Redis dataset to demonstrate the usage of Petrify. First, start a Redis server in AOF mode:

    $ echo "appendonly yes" > redis.conf
    $ redis-server redis.conf

Start Petrify against the file:
    
    $ export AWS_ACCESS_KEY_ID=...
    $ export AWS_SECRET_ACCESS_KEY=...
    $ bin/petrify-persist --bucket petrify --prefix example --file appendonly.aof --interval 4

Add some data to the Redis server over the course of a minute or so:

    $ redis-cli set one   first
    $ redis-cli set two   second
    $ redis-cli set three third
    $ redis-cli set four  fourth

Shutdown the Petrify process, shutdown the Redis process, and remove the local append-only file:

    $ kill -TERM $PETRIFY_PID
    $ kill -TERM $REDIS_PID
    $ rm appendonly.aof

Recover the append-only file to the last stored delta:

    $ bin/petrify-recover --bucket petrify --prefix example --file appendonly.aof

Restart the Redis server against the recovered state:

    $ redis-server redis.conf

Verify that all data was successfully recovered:

    $ redis-cli keys '*'

Instead of recovering to the time of the last stored delta, we can recover to a specific point in time. Start by killing the Redis server again:

    $ kill -TERK $REDIS_PID
    $ rm appendonly.aof

List available recovery timestamps:

    $ bin/petrify-list --bucket petrify --prefix example

Choose an intermediate time at which only a subset of the keys had been added:

    $ bin/petrify-recover --bucket petrify --prefix example --file appendonly.aof --at-timestamp <time>
    $ redis-server redis.conf
    $ redis-cli keys '*'


## Installation

    $ git clone git://github.com/mmcgrana/petrify.git
    $ cd petrify
    $ bundle install
