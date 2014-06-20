var defaultWorkerOptions = require('./default_worker_options');
var EventEmitter = require('events').EventEmitter;
var scripts = require('./scripts');
var Client = require('./client');
var extend = require('xtend');
var Redis = require('redis');

module.exports = createWorker;

function createWorker(queueName, workerFn, options) {

  // PENDING: recover stalled queue
  // PENDING: process timeouts

  var self = new EventEmitter();

  options = extend({}, defaultWorkerOptions, options || {});

  var client = Client(queueName, options);

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
    } else onReady();


    listen();
  }

  function onReady() {
    self.emit('ready');

    listen();
  }

  function listen() {
    if (! stopping && ! listening && pending < options.maxConcurrency) {

      self.emit('listening');
      listening = true;

      options.client.brpoplpush(queues.pending, queues.stalled, options.popTimeout / 1e3, onPop);
    }
  }

  function onPop(err, workId) {
    console.log('popped:', workId);
    listening = false;
    var work;

    setImmediate(listen);

    if (err) error(err);
    else options.client.hgetall(queueName + '#' + workId, gotWork);

    function gotWork(err, _work) {
      if (err) error(err);
      else if(_work) {
        work = _work;
        options.client.zadd(queues.timeout, Date.now() + work.timeout, workId, done);
      }
    }

    function done(err) {
      if (err) error(err);
      else {
        pending ++;
        workerFn.call(null, JSON.parse(work.payload), onWorkerFinished);
      }
    }

    function onWorkerFinished(err) {
      pending --;
      if (err) client.repush(work);
      else dequeue(work.id);
    }
  }


  /// dequeue

  function dequeue(id) {
    options.client.multi().
      lrem(queues.stalled, id).
      del(queueName + '#' + id).
      zrem(queues.timeout, id).
      exec(errorIfError);
  }


  /// Stop

  function stop(cb) {
    console.log('stop');
    if (stopping) return cb();
    stopping = true;
    if (options.client) options.client.quit();
    if (cb) process.nextTick(cb);
  }


  /// Misc

  function errorIfError(err) {
    if (err) error(err);
  }

  function error(err) {
    self.emit('error', err);
  }
}
