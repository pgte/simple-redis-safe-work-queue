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
    pending: queueName + '-pending'
  }
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
    }
  }

  function onReady() {
    self.emit('ready');
  }


  /// Raw Push

  function rawPush(work, cb) {
    options.client.multi().
      hmset(queueName + '#' + work.id, work).
      lpush(queues.pending, work.id).
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
      retried: 0
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
    work.retried ++;
    rawPush(work, cb);
  }


  /// Stop

  function stop() {
    options.client.quit();
  }

}

