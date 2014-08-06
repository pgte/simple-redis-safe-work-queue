var uuid = require('node-uuid').v4;
var test = require('tap').test;
var Queue = require('../')

var queue = uuid();
var client, worker;

var workerOptions = {
  popTimeout: 1,
  autoListen: true
};

test('create client', function(t) {
  client = Queue.client(queue);
  client.once('ready', t.end.bind(t));
});

test('creates worker with autoListen and gets an error if .fetch() is called', function(t) {
  worker = Queue.worker(queue, work, workerOptions);
  worker.on('error', function (err) {
    t.ok(err, 'An error is emitted');
    t.end();
  });
  worker.fetch(work);

  function work(payload, cb) {
    cb();
  }
});

test('stop client', function(t) {
  client.stop(t.end.bind(t));
});

test('stop worker', function(t) {
  worker.stop(t.end.bind(t));
});
