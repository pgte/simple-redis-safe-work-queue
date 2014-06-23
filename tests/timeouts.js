var uuid = require('node-uuid').v4;
var test = require('tap').test;
var Queue = require('../')

var queue = uuid();
var client, worker;

var clientOptions = {
  defaultTimeout: 1000
};

var workerOptions = {
  popTimeout: 1,
  maxRetries: 10,
  maxConcurrency: Infinity
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

  var seen = {};
  var processed = [];

  function work(payload, cb) {
    if (! seen[payload]) {
      seen[payload] = true;
    } else {
      processed.push(payload);
      cb();
    }
  }

  var requeued = [];
  worker.on('timeout requeued', function(workId) {
    requeued.push(workId);
  });

  setTimeout(function() {
    t.equals(requeued.length, 10);
    t.deepEquals(processed.sort(sortNumber), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    t.end();
  }, 4000);

});

test('stop', function(t) {
  client.stop();
  worker.stop();
  t.end();
});

function sortNumber(a,b) {
  return a - b;
}