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

  queueName = queueName + '-pending';
  timeoutName = queueName + '-timeouts';

  init();

  self.push = push;

  return self;


  /// Init

  function init() {
    if (! options.client) {
      options.client = Redis.createClient(options.port, options.host, options.redisOptions);
      if (options.password) options.auth(options.password);
    }
  }


  /// Push

  function push(work, options, cb) {
    if (! work.id) work.id = uuid();
    if (arguments.length == 2 && (typeof options) == 'function') {
      cb = options;
      options = {};
    }

    if (! options) options = {};

    var timeout = Date.now() + (options.timeout || defaultTimeout);

    self.emit('before push', work);

    options.client.multi()
      .zadd(timeoutName, timeout, work.id)
      .lpush(queueName, stringify(work))
      .exec(done);

    function done(err) {
      if (err) {
        if (cb) cb(err);
        else self.emit('error', err);
      } else {
        self.emit('after push', work);
        if (cb) cb();
      }
    }
  }

}

