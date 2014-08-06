var uuid = require('node-uuid').v4;
var test = require('tap').test;
var Queue = require('../')

var queue = uuid();
var client, worker;

var workerOptions = {
  popTimeout: 1,
  maxConcurrency: 1
}

var timeout = 300;
var workCount = 20;
var dueTime = (workCount + 2) * timeout;


test('create client and push work', function(t) {
  client = Queue.client(queue);
  for(var i = 0 ; i < workCount; i ++) {
    client.push(i);
  }
  t.end();
});

test('worker respects max concurrency', function(t) {
  worker = Queue.worker(queue, work, workerOptions);

  var processing = false;
  var processed = 0;

  function work(payload, cb) {
    t.notOk(processing, 'shouldn\'t be processing');
    processed ++;
    processing = true;
    setTimeout(function() {
      processing = false;
      cb();
    }, timeout);
  }

  setTimeout(function() {
    t.equal(processed, workCount);
    t.end();
  }, dueTime);

});

test('stop', function(t) {
  client.stop();
  worker.stop();
  t.end();
});
