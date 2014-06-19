var defaultWorkerOptions = require('./default_worker_options');
var EventEmitter = require('events').EventEmitter;
var extend = require('xtend');
var Redis = require('redis');

module.exports = createWorker;

function createWorker(queueName, workerFn, options) {

  var self = new EventEmitter();

  options = extend({}, defaultWorkerOptions, options || {});

  queues = {
    pending: queueName + '-pending',
    working: queueName + '-working'
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

    /// push every message on dataunits-dead to dataunits
    client.lrange(queueName + '-dead', 0, -1, function(err, workUnits) {
      if (err) return cb(err);

      async.each(workUnits, function(workUnit, cb) {
        client.lpush(queueName, workUnit, cb);
      }, done);
    });

    function done(err) {
      if (err) self.error(err);
      else onRestored();
    }
  }

  function onRestored() {
    self.emit('restored');
    listen();
  }

  function listen() {
    if (! stopping && ! listening && pending < options.maxConcurrency) {
      self.emit('listening');
      listening = true;
      options.client.brpoplpush(queues.pending, queues.working, options.popTimeout, onPop);
    }
  }

  function onPop(err, dataunit) {
    listening = false;
    if (err) error(err);
    if (dataunit) {
      pending ++;

      setImmediate(listen);

      dataunit = JSON.parse(dataunit);
      var taskId = dataunit.t || dataunit.taskId;
      var blob = 'b' in dataunit ? dataunit.b : dataunit.blob;

      try {

        assert(taskId, 'need task id');
        assert(blob != undefined, 'need blob');

      } catch(err) {
        return loopError(err, dataunit);
      }

      backend.dataunits.create(taskId, blob, onDataunitCreated);

      function onDataunitCreated(err, dataunit) {
        if (err) {
          loopError(err, dataunit);
        } else {
          /// Remove from the dataunits-dead queue
          client.rpop('dataunits-dead', onDataunitsDeadPop);
        }
      }

    }
  }

  /// Stop

  function stop(cb) {
    if (stopping) return cb();
    stopping = true;
    if (options.client) options.client.quit();
    if (cb) process.nextTick(cb);
  };
}

