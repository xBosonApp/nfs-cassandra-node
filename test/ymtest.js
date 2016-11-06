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

var WAIT_INTER = { message: 'wait function' };


module.exports = run;


//
// 独立成项目
//
function run(conf, out) {
  if (!out) out = console;

  var step      = [];
  var total     = 0;
  var succ      = 0;
  var start     = Date.now();
  var logs      = [ '\n[ Test console log ]', LINE ];
  var log_len   = logs.length;
  var blank     = '  ';
  var finished  = {};
  var ploop_cnt = 0;
  var eq_cnt    = 0;
  var event     = new Events();
  var note      = conf.note || '';
  event.start   = _start;

  for (var n in conf) {
    if (n && typeof conf[n] == 'function') {
      step.push(n);
    }
  }
  ploop_cnt = total = step.length;

return event;

  function _start() {
    console.log('\n[ Begin Test', note, ']');
    console.log(LINE);
    do_test();
  }

  function do_test() {
    var fnn = step.pop();
    //console.log('!', fnn, eq_cnt, step.length, total);

    if (ploop_cnt > step.length) {
      ploop_cnt = step.length;
      eq_cnt = 0;
    } else {
      if (++eq_cnt >= total + total) {
        // throw Error('循环依赖!');
      }
    }

    if (fnn) {
      var test = createTest(fnn);
      try {
        conf[fnn](test);
      } catch(e) {
        if (e === WAIT_INTER) {
          return;
        }
        test.assert(e);
        test.finish();
      }
    } else {
      all_finish();
    }
  }


  function all_finish() {
    blank = '';
    console.log();
    var msg = 'Test ' + succ + '/' + total
      + ' passed, use ' + (Date.now() - start) + 'ms.';
    if (succ != total) {
      fail(msg);
    } else {
      ok(msg);
    }
    if (logs.length > log_len) {
      for (var i=0, e=logs.length; i<e; ++i) {
        out.log(logs[i]);
      }
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
      ++ploop_cnt;
      eq_cnt = 0;
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
      setImmediate(do_test);
    }

    function assert(value, msg) {
      if (_finish) throw new Error('what ? is finished!');
      if (util.isError(value)) {
        haserr = true;
        log(value.message || value);
        value.stack && log(value.stack);
      } else if (!value) {
        if (!msg) return;
        haserr = true;
        log(msg || 'fail');
      }
    }

    function wait(_name) {
      if (_finish) throw new Error('what ? is finished!');
      if (arguments.length > 1) {
        for (var i=arguments.length-1; i>=0; --i) {
          wait(arguments[i]);
        }
        return;
      }

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
        return;
      }
      _finish = true;
      step.splice(0, 0, name);
      setImmediate(do_test);
      // return false;
      throw WAIT_INTER;
    }
  }

  function fail(msg) {
    console.log(blank + color.bold_prefix + color.error_prefix
      + FAIL + ' ' + msg + color.error_suffix + color.bold_suffix);
  }

  function ok(msg) {
    console.log(blank + color.ok_prefix
      + PASS + ' ' + msg + color.ok_suffix);
  }
}

process.on('uncaughtException', function (err) {
  if (err === WAIT_INTER) {
    return;
  } if (err && err.context && err.context === WAIT_INTER) {
    return;
  }
  console.error('uncaughtException', err);
  process.exit(1);
});
