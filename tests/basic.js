var uuid = require('uuid').v4;
var test = require('tap').test;
var Queue = require('../')

var queue = uuid();
var client, worker;

var workerOptions = {
  popTimeout: 1
}

test('create client', function(t) {
  client = Queue.client(queue);
  client.once('ready', t.end.bind(t));
});

test('push work without callback', function(t) {
  client.push({a:1, b:2});
  t.end();
});

test('push work with callback', function(t) {
  client.push({c:3, d:4}, pushed);

  function pushed(err) {
    if (err) throw err;
    t.end();
  }

});

test('creates worker that gets work', function(t) {
  worker = Queue.worker(queue, work, workerOptions);

  var times = 0;
  var payloads = [];

  function work(payload, cb) {
    payloads.push(payload);
    cb();
    if (++ times == 2) setTimeout(finished, 500);
  }

  function finished() {
    t.deepEqual(payloads, [{a:1, b:2}, {c:3, d:4}]);
    t.end();
  }

});

test('stop client', function(t) {
  client.stop(t.end.bind(t));
});

test('stop worker', function(t) {
  worker.stop(t.end.bind(t));
});