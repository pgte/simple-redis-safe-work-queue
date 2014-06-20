var defaultWorkerOptions = require('./default_worker_options');
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

  process.nextTick(init);

  self.stop = stop;

  return self;


  /// Init

  function init() {
    if (! options.client) {
      options.client = Redis.createClient(options.port, options.host, options.redisOptions);
      if (options.password) options.auth(options.password);
      options.client.once('ready', onReady);
    } else {
      readies ++;
    }

    client = Client(queueName, options);
    client.once('ready', onReady);

    listen();
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
    else if (workId) {
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
        work.payload = JSON.parse(work.payload);
        work.retried = Number(work.retried);
        client.client.zadd(queues.timeout, Date.now() + work.timeout, workId, done);
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
      lrem(queues.stalled, 1, id).
      del(queueName + '#' + id).
      zrem(queues.timeout, id).
      exec(cb);
  }


  /// Stop

  function stop(cb) {
    if (stopping) return cb();
    stopping = true;
    options.client.quit();
    options.client.once('end', ended);

    client.stop();
    client.once('end', ended);

    var endedCount = 0;
    function ended() {
      if (++ endedCount == 2) {
        self.emit('end');
        if (cb) cb();
      }
    }
  }


  /// Misc

  function errorIfError(err) {
    if (err) error(err);
  }

  function error(err) {
    if (Array.isArray(err)) err.forEach(error);
    else self.emit('error', err);
  }
}
