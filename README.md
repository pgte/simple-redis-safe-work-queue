# Client

## Client options:

* port: 6379
* host: "127.0.0.1"
* defaultTimeout: the default worker timeout, in miliseconds. defaults to 60000 (60 seconds)

## Client events:

* `emit('before push', work)` - before work is pushed
* `emit('after push', work)` - after work is successfully pushed
* `emit('error', err)` - when a push error happens and a callback wasn't provided


# Worker

## Worker options:

* port: redis port (defaults to 6379)
* host: redis host (defaults to "127.0.0.1")
* password: redis password
* redisOptions: any option allowed by the [redis client](https://github.com/mranney/node_redis)
* client: a redis client instance (optional, if you want to create / reuse your own client)
* maxConcurrency: the maximum pending work units. defaults to 10.
* popTimeout: the worker pop timeout, after which it retries, in seconds. Defaults to 3 seconds.

## Worker Events:

* `emit('ready')`: when redis client is ready
* `emit('listening')`: when listening for work
* `emit('worker error', err)`: when a worker error occurs
* `emit('work done', payload)`: when a worker finishes a piece of work