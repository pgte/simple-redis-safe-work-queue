var uuid = require('uuid').v4;
var test = require('tap').test;
var Queue = require('../');

var queue = uuid();
var client, worker;

var workerOptions = {
  popTimeout: 1,
  autoListen: false
};

test('create client', function(t) {
  client = Queue.client(queue);
  client.once('ready', t.end.bind(t));
});

test('push work', function(t) {
  client.push({a:1, b:2});
  client.push({a:1, b:2});
  client.push({a:1, b:2});
  t.end();
});

test('creates worker without autoListen and two pieces of work are received when fetch is called twice', function(t) {
  worker = Queue.worker(queue, work, workerOptions);

  worker.on('listening', function () {
    t.notOk(true, 'Listen has been called when only fetch has been used');
    t.end();
  });

  setTimeout(finished, 5000);

  var payloads = [];

  function work(payload, cb) {
    payloads.push(payload);
    cb();
  }

  function callListen() {
    worker.listen();
    listenCalled = true;
  }

  function finished() {
    t.deepEqual(payloads, [{a:1, b:2}]);
    t.end();
  }

  worker.fetch();
  worker.fetch();
});

test('stop client', function(t) {
  client.stop(t.end.bind(t));
});

test('stop worker', function(t) {
  worker.stop(t.end.bind(t));
});
