var fs = require('fs');
var crypto = require('crypto');

exports.scripts = {};
exports.shas = {};

['timeout'].forEach(load);

function load(scriptName) {
  var script =
    exports.scripts[scriptName] =
    fs.readFileSync(__dirname + '/' + scriptName + '.lua', {encoding: 'utf8'});

  exports.shas[scriptName] = crypto.createHash("sha1").update(script).digest("hex");
}

exports.run = function() {
  var callback;
  var self = this;

  var args = Array.prototype.slice.call(arguments);
  if (typeof args[args.length - 1] == "function")
    callback = args.pop();

  if (Array.isArray(args[0])) args = args[0];

  var scriptName = args[0];
  var sha = exports.shas[scriptName];

  if (! sha) callback(new Error('no such script: ' + scriptName));

  args[0] = sha;
  self.evalsha(args, evalShaReplied);

  function evalShaReplied(err, reply) {
    if (err && /NOSCRIPT/.test(err.message)) {
      args[0] = exports.scripts[scriptName];
      self.eval(args, callback);
    } else if (callback) {
      callback(err, reply);
    }
  }
};