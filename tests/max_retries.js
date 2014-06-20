var uuid = require('node-uuid').v4;
var test = require('tap').test;
var Queue = require('../')

var queue = uuid();
var client, worker;

var workerOptions = {
  popTimeout: 1,
  maxRetries: 2
}

var workCount = 10;

test('create client and push work', function(t) {
  client = Queue.client(queue);
  for(var i = 0 ; i < workCount; i ++) {
    client.push(i + 1);
  }
  t.end();
});

test('worker retries for maxRetries', function(t) {
  worker = Queue.worker(queue, work, workerOptions);

  var processed = 0;

  function work(payload, cb) {
    processed ++;
    process.nextTick(function() {
      cb(new Error('something awful has happened'));
    });
  }

  var maxRetries = 0;
  worker.on('max retries', function(err, payload) {
    t.equal(++ maxRetries, payload);
  });

  setTimeout(function() {
    t.equal(maxRetries, workCount);
    t.equal(processed, workerOptions.maxRetries * workCount);
    t.end();
  }, 2000);

});

test('stop', function(t) {
  client.stop();
  worker.stop();
  t.end();
});