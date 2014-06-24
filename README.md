# Client

[![Build Status](https://travis-ci.org/pgte/simple-redis-safe-work-queue.svg)](https://travis-ci.org/pgte/simple-redis-safe-work-queue)

## Client options:

Example of a client producing work:

```javascript
var Queue = require('simple-redis-safe-work-queue');

client = Queue.client('send-emails');
client.push({to: 'someone@somewhere.com', subject: 'hey there', body: 'yo'});
```

or, with a callback for when the work is pushed:

```javascript
client.push({to: 'someone@somewhere.com', subject: 'hey there', body: 'yo'}, function(err) {
  if (err) console.error('Error when pushing work into the queue: ', err.stack);
});
```

Client also accepts options as second argument in constructor:

* port: 6379
* host: "127.0.0.1"
* defaultTimeout: the default worker timeout, in miliseconds. defaults to 60000 (60 seconds)

## Client events:

Client emmits the following events:

* `emit('before push', work)` - before work is pushed
* `emit('after push', work)` - after work is successfully pushed
* `emit('error', err)` - when a push error happens and a callback wasn't provided


# Worker

Example of a worker consuming work:

```javascript
var Queue = require('simple-redis-safe-work-queue');

worker = Queue.worker('send-email', workFunction);

function workFunction(email, cb) {
  sendEmail(email, function(err) {
    if (err) cb(err);
    else {
      console.log('email send successfully, calling back with no errors');
      cb();
    }
  });
}
```

## Worker options:

You can pass some options on the third argument of the worker constructor:

* port: redis port (defaults to 6379)
* host: redis host (defaults to "127.0.0.1")
* password: redis password
* redisOptions: any option allowed by the [redis client](https://github.com/mranney/node_redis)
* maxConcurrency: the maximum pending work units. defaults to 10.
* popTimeout: the worker pop timeout, after which it retries, in seconds. Defaults to 3 seconds.
* runTimeoutWatchdog: run a timeout watchdog, defaults to `true`
* runStalledWatchdog: run a stalled watchdog, defaults to `true`

## Worker Events:

A worker emits the following events:

* `emit('ready')`: when redis client is ready
* `emit('listening')`: when listening for work
* `emit('worker error', err)`: when a worker error occurs
* `emit('work done', payload)`: when a worker finishes a piece of work
* `emit('repush', payload)`: when a work unit is being repushed after failure
* `emit('max retries', lastError, payload)`: when the maximum retries has been reached

# Testing

Clone this repo, enter the repo directory, start a redis server and run:

```bash
$ npm test
```

# License

MIT