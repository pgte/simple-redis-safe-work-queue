var uuid = require('uuid').v4;
var test = require('tap').test;
var Queue = require('../')

var queue = uuid();
var client, worker;

var clientOptions = {
  defaultTimeout: 2000
};

var workerOptions = {
  popTimeout: 1,
  maxRetries: 10,
  maxConcurrency: Infinity
}

var workCount = 10;
var workItems = [];

test('create client and push work', function(t) {
  client = Queue.client(queue, clientOptions);
  for(var i = 0 ; i < workCount; i ++) {
    var payload = i + 1;
    client.push(payload);
    workItems.push(payload);
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
    t.equals(requeued.length, workCount);
    t.deepEquals(processed.sort(sortNumber), workItems);
    t.end();
  }, 6000);

});

test('stop', function(t) {
  client.stop();
  worker.stop();
  t.end();
});

function sortNumber(a,b) {
  return a - b;
}