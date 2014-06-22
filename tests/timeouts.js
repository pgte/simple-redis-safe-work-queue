var uuid = require('node-uuid').v4;
var test = require('tap').test;
var Queue = require('../')

var queue = uuid();
var client, worker;

var clientOptions = {
  defaultTimeout: 100
};

var workerOptions = {
  popTimeout: 1,
  maxRetries: 10
}

var workCount = 10;

test('create client and push work', function(t) {
  client = Queue.client(queue, clientOptions);
  for(var i = 0 ; i < workCount; i ++) {
    client.push(i + 1);
  }
  t.end();
});

test('timeout kicks in', function(t) {
  worker = Queue.worker(queue, work, workerOptions);

  var failing = true;
  var processed = 0;

  function work(payload, cb) {
    console.log('work', payload);
  }

  var requeued = [];
  worker.on('timeout requeued', function(workId) {
    requeued.push(workId);
  });

});

test('stop', function(t) {
  client.stop();
  worker.stop();
  t.end();
});