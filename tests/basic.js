var test = require('tap').test;
var Queue = require('../')

var client;

test('create client', function(t) {
  client = Queue.createClient();
  client.once('ready', t.end.bind(t));
});

test('push work without callback', function(t) {
  client.push({a:1, b:2});
  t.end();
});

test('push work with callback', function(t) {
  client.push({c:3, d:4}, pushed);

  function pushed(err) {
    if (err) throw err;
    t.end();
  }

});

// test('create worker', function() {

// });