var defaultOptions = require('./default_watchdog_options');
var EventEmitter = require('events').EventEmitter;
var scripts = require('./scripts');
var extend = require('xtend');
var Redis = require('redis');

module.exports = createWatchdog;

function createWatchdog(queueName, options) {

  var self = new EventEmitter();

  options = extend({}, defaultOptions, options || {});

  var queues = {
    pending: queueName + '-pending',
    timeout: queueName + '-timeout',
    stalled: queueName + '-stalled',
  };

  /// state vars
  var listening = false;
  var stopping = false;

  process.nextTick(init);

  self.stop = stop;

  return self;


  /// Init

  function init() {
    options.client = Redis.createClient(options.port, options.host, options.redisOptions);
    if (options.password) options.auth(options.password);
    options.client.once('ready', onReady);
    options.client.on('error', errorIfError);
  }

  function onReady() {
    self.emit('ready');
    poll();
  }

  function poll() {
    if (! stopping && ! listening) {
      listening = true;
      self.emit('polling');

      scripts.run.call(options.client, 'timeout', 1,
        queueName,
        queues.timeout, queues.pending, queues.stalled, Date.now(),
        done);
    }
  }

  function done(err, items) {
    listening = false;

    setTimeout(poll, options.pollInterval);

    if (items && items.length) {
      items.forEach(function(item) {
        self.emit('timeout requeued', item);
      });
    }

    if (err) self.emit('error', err);
  }


  /// Stop

  function stop(cb) {
    if (stopping) return cb();
    stopping = true;
    options.client.quit();
    options.client.once('end', ended);

    function ended() {
      self.emit('end');
      if (cb) cb();
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