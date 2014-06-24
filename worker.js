var defaultWorkerOptions = require('./default_worker_options');
var TimeoutWatchdog = require('./timeout_watchdog');
var StalledWatchdog = require('./stalled_watchdog');
var EventEmitter = require('events').EventEmitter;
var Client = require('./client');
var extend = require('xtend');
var Redis = require('redis');

module.exports = createWorker;

function createWorker(queueName, workerFn, options) {

  // PENDING: recover stalled queue
  // PENDING: process timeouts

  var self = new EventEmitter();

  if (typeof options == 'number') {
    options = {
      maxConcurrency: options
    };
  }

  options = extend({}, defaultWorkerOptions, options || {});

  var client;

  // watchdogs
  var watchdogs = {};
  if (options.runTimeoutWatchdog) {
    watchdogs.timeout = TimeoutWatchdog(queueName, options);
    watchdogs.timeout.on('error', errorIfError);
    watchdogs.timeout.on('timeout requeued', function(item) {
      self.emit('timeout requeued', item);
    });
  }
  if (options.runStalledWatchdog) {
    watchdogs.stalled = StalledWatchdog(queueName, options);
    watchdogs.stalled.on('error', errorIfError);
    watchdogs.stalled.on('stalled requeued', function(item) {
      self.emit('stalled requeued', item);
    });
  }

  var readies = 0;

  var queues = {
    pending: queueName + '-pending',
    timeout: queueName + '-timeout',
    stalled: queueName + '-stalled',
  };

  /// state vars
  var listening = false;
  var stopping = false;
  var pending = 0;

  self.stop = stop;

  init();

  return self;


  /// Init

  function init() {
    options.client = Redis.createClient(options.port, options.host, options.redisOptions);
    if (options.password) options.auth(options.password);
    options.client.once('ready', onReady);

    options.client.on('error', errorIfError);

    client = Client(queueName, options);
    client.on('error', error);
    client.once('ready', onReady);
  }

  function onReady() {
    if (++ readies == 2) {
      self.emit('ready');

      listen();
    }
  }

  function listen() {
    if (! stopping && ! listening && pending < options.maxConcurrency) {
      self.emit('listening');
      listening = true;

      options.client.brpoplpush(queues.pending, queues.stalled, options.popTimeout, onPop);
    }
  }

  function onPop(err, workId) {
    listening = false;
    var work;

    setImmediate(listen);

    if (err && ! stopping) error(err);
    else if (workId && ! options.stall) {
      pending ++;
      client.client.hgetall(queueName + '#' + workId, gotWork);
    }

    function gotWork(err, _work) {
      if (err) {
        pending --;
        error(err);
      } else if(_work) {
        work = _work;
        work.tried = Number(work.tried);
        work.retried = Number(work.retried);
        work.timeout = Number(work.timeout);
        work.payload = JSON.parse(work.payload);

        client.client.multi().
          zrem(queues.timeout, workId).
          zadd(queues.timeout, Date.now() + work.timeout, workId).
          lrem(queues.stalled, 1, workId).
          exec(done);
      }
    }

    function done(err) {
      if (err) {
        pending --;
        error(err);
      } else {
        work.tried ++;
        workerFn.call(null, work.payload, onWorkerFinished);
      }
    }

    function onWorkerFinished(err) {
      if (err) {
        self.emit('worker error');
        pending --;
        maybeRetry(err, work);
        listen();
      } else {
        self.emit('work done', work);
        dequeue(work.id, dequeued);
      }
    }
  }

  function maybeRetry(err, work) {
    if (work.tried < options.maxRetries) {
      self.emit('repush', work.payload);
      client.repush(work);
    } else {
      dequeue(work.id, errorIfError);
      self.emit('max retries', err, work.payload);
    }
  }

  function dequeued(err) {
    pending --;
    if (err) error(err);
    listen();
  }


  /// dequeue

  function dequeue(id, cb) {
    client.client.multi().
      del(queueName + '#' + id).
      zrem(queues.timeout, id).
      exec(cb);
  }


  /// Stop

  function stop(cb) {
    if (stopping) {
      if (cb) cb();
      return;
    }

    stopping = true;
    options.client.quit();
    options.client.once('end', stopped);
    client.stop(stopped);
    var waitingFor = 2;

    var roles = Object.keys(watchdogs);
    waitingFor += roles.length;
    roles.forEach(function(role) {
      watchdogs[role].stop(stopped);
    });

    var endedCount = 0;
    function stopped() {
      if (++ endedCount == waitingFor) {
        self.emit('end');
        if (cb) cb();
      }
    }
  }


  /// Misc

  function errorIfError(err) {
    if (err && ! stopping) error(err);
  }

  function error(err) {
    if (Array.isArray(err)) err.forEach(error);
    else self.emit('error', err);
  }
}
