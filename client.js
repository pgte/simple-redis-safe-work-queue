var defaultClientOptions = require('./default_client_options');
var EventEmitter = require('events').EventEmitter;
var stringify = require('json-stringify-safe');
var uuid = require('node-uuid').v4;
var extend = require('xtend');
var Redis = require('redis');

module.exports = createClient;

function createClient(queueName, options) {

  var self = new EventEmitter();

  options = extend({}, defaultClientOptions, options || {});

  var queues = {
    pending: queueName + '-pending',
    stalled: queueName + '-stalled',
    timeout: queueName + '-stalled'
  }

  var stopping = false;

  init();

  self.push = push;
  self.repush = repush;
  self.stop = stop;

  return self;


  /// Init

  function init() {
    if (! options.client) {
      options.client = Redis.createClient(options.port, options.host, options.redisOptions);
      if (options.password) options.auth(options.password);
      options.client.once('ready', onReady);
    } else {
      self.emit('ready');
    }
    options.client.on('error', error);
    self.client = options.client;
  }

  function onReady() {
    self.emit('ready');
  }


  /// Raw Push

  function rawPush(work, cb) {
    options.client.multi().
      hmset(queueName + '#' + work.id, work).
      lrem(queues.pending, 1, work.id).
      lpush(queues.pending, work.id).
      zrem(queues.timeout, work.id).
      exec(done);

    function done(err) {
      if (err) {
        if (cb) cb(err);
        else self.emit('error', err);
      } else if (cb) cb();
    }
  }


  /// Push

  function push(payload, pushOptions, cb) {
    var id = uuid();
    if (arguments.length == 2 && (typeof pushOptions) == 'function') {
      cb = pushOptions;
      pushOptions = {};
    }

    if (! pushOptions) pushOptions = {};

    var work = {
      id: id,
      timeout: pushOptions.timeout || options.defaultTimeout,
      payload: stringify(payload),
      tried: 0
    };

    self.emit('before push', work);

    rawPush(work, pushed);

    function pushed(err) {
      if (! err) self.emit('after push', work);
      if (cb) cb(err);
    }
  }


  ///  Repush

  function repush(work, cb) {
    rawPush(work, cb);
  }


  /// Stop

  function stop(cb) {
    if (stopping) return cb();
    stopping = true;
    options.client.quit();
    options.client.once('end', function() {
      self.emit('end');
      if (cb) cb();
    });
  }


  /// Error

  function error(err) {
    if (err) self.emit('error', err);
  }

}
