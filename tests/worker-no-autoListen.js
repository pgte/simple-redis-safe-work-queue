var uuid = require('node-uuid').v4;
var test = require('tap').test;
var Queue = require('../')

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
  t.end();
});

test('creates worker without autoListen and no work is received automatically until listen is called', function(t) {
  worker = Queue.worker(queue, work, workerOptions);

  var listenCalled = false;
  setTimeout(callListen, 5000);

  var times = 0;
  var payloads = [];

  function work(payload, cb) {
    if (!listenCalled) {
      t.notOk(false, 'Work received when autoListen is disabled');
      t.end();
    }
    else {
      payloads.push(payload);
      cb();
      setTimeout(finished, 500);
      t.end();
    }
  }

  function callListen() {
    worker.listen();
    listenCalled = true;
  }

  function finished() {
    t.deepEqual(payloads, [{a:1, b:2}]);
    t.end();
  }
});

test('stop client', function(t) {
  client.stop(t.end.bind(t));
});

test('stop worker', function(t) {
  worker.stop(t.end.bind(t));
});
