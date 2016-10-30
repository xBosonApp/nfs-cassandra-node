var assertlib = require('assert');
var Events    = require('events');
var util      = require('util');


var PASS = process.platform === 'win32' ? '\u221A' : '✔';
var FAIL = process.platform === 'win32' ? '\u00D7' : '✖';
var LINE = '-----------------------------------------------';

var color = {
    "error_prefix"      : "\u001B[31m",
    "error_suffix"      : "\u001B[39m",
    "ok_prefix"         : "\u001B[32m",
    "ok_suffix"         : "\u001B[39m",
    "bold_prefix"       : "\u001B[1m",
    "bold_suffix"       : "\u001B[22m",
    "assertion_prefix"  : "\u001B[35m",
    "assertion_suffix"  : "\u001B[39m"
};


module.exports = run;


//
// 独立成项目
//
function run(conf, out) {
  if (!out) out = console;

  var step   = [];
  var total  = 0;
  var succ   = 0;
  var start  = Date.now();
  var logs   = [ '\n[ Test console log ]', LINE ];
  var finished = {};

  var event  = new Events();
  event.start = _start;

  for (var n in conf) {
    n && step.push(n);
  }
  total = step.length;

return event;

  function _start() {
    console.log('\n[ Begin Test ]');
    console.log(LINE);
    do_test();
  }

  function do_test() {
    var fnn = step.pop();
    if (fnn) {
      var test = createTest(fnn);
      try {
        conf[fnn](test);
      } catch(e) {
        test.assert(e);
        test.finish();
      }
    } else {
      all_finish();
    }
  }


  function all_finish() {
    console.log();
    var msg = 'Test ' + succ + '/' + total
      + ' passed, use ' + (Date.now() - start) + 'ms.';
    if (succ != total) {
      fail(msg);
    } else {
      ok(msg);
    }
    for (var i=0, e=logs.length; i<e; ++i) {
      out.log(logs[i]);
    }
    event.emit('finish');
  }


  function createTest(name) {
    var haserr = false;
    var _finish = false;
    return {
      finish : finish,
      done   : finish,
      end    : finish,
      assert : assert,
      wait   : wait,
      log    : log,
      info   : log,
      warn   : log,
      error  : log,
      retest : retest,
    }

    function retest(_name) {
      if (_finish) throw new Error('what ? is finished!');
      if (!conf[_name]) {
        throw new Error('config ' + name + ' not exists');
      }
      ++total;
      step.push(_name);
    }

    function log() {
      logs.push('  ' + name + ':');
      var str = ['    '];
      for (var i=0, e=arguments.length; i<e; ++i) {
        if (!arguments[i]) continue;
        var obj = JSON.stringify(arguments[i]);
        for (var j=0, je=obj.length; j<je; ++j) {
          str.push(obj[j]);
          if (str.length > 75) {
            logs.push(str.join(''));
            str = ['    '];
          }
        }
        str.push(' ');
      }
      logs.push(str.join(''));
      str = ['    '];
    }

    function finish() {
      if (_finish) throw new Error('what ? is finished!');
      if (haserr) {
        fail(name);
      } else {
        ok(name);
        ++succ;
      }
      finished[name] = true;
      do_test();
    }

    function assert(value, msg) {
      if (_finish) throw new Error('what ? is finished!');
      if (util.isError(value)) {
        haserr = true;
        log(value.message || value);
      } else if (!value) {
        if (!msg) return;
        haserr = true;
        log(msg);
      }
    }

    function wait(_name) {
      if (arguments.length > 1) {
        var ret = true;
        for (var i=arguments.length-1; i>=0; --i) {
          if (! wait(arguments[i]) ) {
            ret = false;
            break;
          }
        }
        return ret;
      }

      if (_finish) throw new Error('what ? is finished!');
      if (!_name) {
        throw new Error('must wait something');
      }
      if (!conf[_name]) {
        throw new Error('config ' + name + ' not exists');
      }
      if (_name == name) {
        throw new Error('cannot wait myself');
      }
      if (finished[_name]) {
        return true;
      }
      _finish = true;
      step.splice(0, 0, name);
      setImmediate(do_test);
      return false;
    }
  }

  function fail(msg) {
    console.log('  ' + color.bold_prefix + color.error_prefix
      + FAIL + ' ' + msg + color.error_suffix + color.bold_suffix);
  }

  function ok(msg) {
    console.log('  ' + color.ok_prefix
      + PASS + ' ' + msg + color.ok_suffix);
  }
}
