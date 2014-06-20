var defaultWorkerOptions = require('./default_worker_options');
var EventEmitter = require('events').EventEmitter;
var scripts = require('./scripts');
var Client = require('./client');
var extend = require('xtend');
var Redis = require('redis');

module.exports = createWorker;

function createWorker(queueName, workerFn, options) {

  var self = new EventEmitter();

  options = extend({}, defaultWorkerOptions, options || {});

  var client = Client(queueName, options);

  var queues = {
    pending: queueName + '-pending',
    timeout: queueName + '-timeout'
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

      scripts.run.call(
        options.client,
        'pop', // script
        3, // 3 keys
        queueName, queues.pending, queues.timeout, // keys
        Date.now(), // arg[0]
        onPop
      );
    }
  }

  function onPop(err, work) {
    listening = false;

    setImmediate(listen);

    if (err) error(err);

    if (work) {
      pending ++;
      workerFn.call(null, JSON.parse(work.payload), onWorkerFinished);
    }

    function onWorkerFinished(err) {
      pending --;
      if (err) client.repush(work);
      else dequeue(work.id);
    }
  }


  /// deqeue

  function dequeue(id) {
    options.client.multi().
      del(queueName + '#' + id).
      zrem(queues.timeout, id).
      exec(errorIfError);
  }


  /// Stop

  function stop(cb) {
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

