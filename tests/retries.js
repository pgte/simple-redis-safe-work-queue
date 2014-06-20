var uuid = require('node-uuid').v4;
var test = require('tap').test;
var Queue = require('../')

var queue = uuid();
var client, worker;

var workerOptions = {
  popTimeout: 1,
  maxRetries: 10
}

var workCount = 10;

test('create client and push work', function(t) {
  client = Queue.client(queue);
  for(var i = 0 ; i < workCount; i ++) {
    client.push(i + 1);
  }
  t.end();
});

test('worker first fails', function(t) {
  worker = Queue.worker(queue, work, workerOptions);

  var failing = true;
  var processed = 0;

  function work(payload, cb) {
    t.equal(++ processed, payload);
    if (failing) {
      process.nextTick(function() {
        cb(new Error('something awful has happened'));
      });
    } else process.nextTick(cb);

    if (processed == workCount) {
      if (failing) {
        processed = 0;
        failing = false;
      } else {
        t.end();
      }
    }
  }

});

test('stop', function(t) {
  client.stop();
  worker.stop();
  t.end();
});