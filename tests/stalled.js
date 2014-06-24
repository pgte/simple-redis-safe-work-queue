var uuid = require('node-uuid').v4;
var test = require('tap').test;
var Queue = require('../')

var queue = uuid();
var client, stallingWorker, worker;

var stallingWorkerOptions = {
  popTimeout: 1,
  stall: true,
  runTimeoutWatchdog: false,
  stalledTimeout: 1e3
}

var workerOptions = {
  popTimeout: 1,
  maxConcurrency: Infinity,
  runTimeoutWatchdog: false
}

var workCount = 10;
var works = [];

test('create client and push work', function(t) {
  client = Queue.client(queue);
  for(var i = 0 ; i < workCount; i ++) {
    var payload = i + 1;
    client.push(payload);
    works.push(payload);
  }
  t.end();
});

test('run a stalling worker', function(t) {
  stallingWorker = Queue.worker(queue, noop, stallingWorkerOptions);

  var requeued = [];

  stallingWorker.on('stalled requeued', function(workId) {
    console.log('requeued', workId);
    requeued.push(workId);
    if (requeued.length == workCount)
      stallingWorker.stop(function() {
        console.log('stalling worker ended');
        t.end();
      });
  });
});

test('stall detection kicks in', function(t) {

  worker = Queue.worker(queue, work, workerOptions);

  var seen = [];
  function work(payload, cb) {
    console.log('work', payload);
    seen.push(payload);
    cb();
  }

  setTimeout(function() {
    t.deepEquals(seen.sort(sortNumber), works);
    t.end();
  }, 10000);

});

test('stop', function(t) {
  client.stop();
  worker.stop();
  t.end();
});

function sortNumber(a,b) {
  return a - b;
}

function noop() {}