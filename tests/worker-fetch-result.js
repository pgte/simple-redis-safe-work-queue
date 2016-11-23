var uuid = require('uuid').v4;
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

test('creates worker without autoListen, fetches a message and gets a response', function(t) {
  worker = Queue.worker(queue, work, workerOptions);
  worker.fetch();

  worker.on('work done', function(work) {
    t.deepEqual(work, {a:1, b:2});
  });

  function work(payload, cb) {
    cb();
    t.deepEqual(payload, {a:1, b:2});
    t.end();
  }
});

test('stop client', function(t) {
  client.stop(t.end.bind(t));
});

test('stop worker', function(t) {
  worker.stop(t.end.bind(t));
});
