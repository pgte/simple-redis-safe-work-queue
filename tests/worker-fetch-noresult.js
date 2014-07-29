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

test('creates worker without autoListen, fetches a message and gets an empty response', function(t) {
  worker = Queue.worker(queue, work, workerOptions);
  worker.fetch(work);

  function work(payload, cb) {
    cb();
    t.equal(payload, null, 'payload should be null');
    t.end();
  }
});

test('stop client', function(t) {
  client.stop(t.end.bind(t));
});

test('stop worker', function(t) {
  worker.stop(t.end.bind(t));
});
