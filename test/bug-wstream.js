var fs_cass   = require('../');

// '937f8100-1b2b-11e7-9d3d-03b23e58f867';
var driverid = '359ed850-1d93-11e7-adcf-411002e049ae'
var wait;


fs_cass.open_driver(function(err, driver) {
  driver.open_fs(driverid, function(err, fs) {
    if (err) {
      console.log(err);
      process.exit(1);
    }
    try {
      wait = wait_stream_end(function() {
        console.log('All over');
        process.exit(0);
      });

      wstream(fs, driver);
    } catch(e) {
      console.log(e);
    }
  });
});

var time = 0;
setInterval(function() {
  console.log('Use', ++time);
}, 1000);


var str = 'qwertyuioplkjhgfdsazxcvbnm1234567890!@#$%^&*()_+|~';
var sl  = str.length;


//
// 写入流 bug 已经修复
//
function wstream(fs, drv) {
  var w1 = one('/a/1.txt', '1', 22);
  var w2 = one('2.txt', '2', 33);
  var w3 = one('3.txt', null, 280);

  while (
       w2.write()
    || w1.write()
    || w3.write()
  );


  function one(filename, ch, len) {
    var w = fs.createWriteStream(filename);
    wait.add_fs_writer(w);

    var blen = 1011;

    var buf = Buffer.alloc(blen);
    for (var i=0; i<blen; ++i) {
      buf[i] = ch || str[i % sl].charCodeAt();
    }
    // console.log(buf)


    w.on('close', function() {
      var fail;
      var over = wait.create_hook();

      fs.readFile(filename, function(e, c) {
        if (e) {
          console.log(filename, e);
        }
        else if (c.length != buf.length*len) {
          console.log('Fail 1:', filename, c.length, buf.length * len);
          fail = true;
        }
        else {
          for (var i=0; i<c.length; ++i) {
            if (c[i] != buf[i%blen]) {
              console.log('Fail 2:', filename, 'offset:', i, c[i], buf[i%blen]);
              fail = true;
              break;
            }
          }
        }

        if (!fail) {
          console.log('Success', filename, c, '\n');
        } else {
          console.log('----->', c , '\n\n');
        }
        over();
      });
    });

    var i = -1;

    return {
      w : w,
      write : function() {
        if (++i<len) {
          w.write(buf);
          return true;
        } else {
          w.end();
        }
      },
    };
  }

  function hex(b) {
    var out = [];
    for (var i=0, e=b.length; i<e; ++i) {
      out.push(b[i] + '');
    }
    return out.join(' ');
  }
}


function wait_stream_end(cb, timeout) {
  var wait = 0;
  var end = false;

  return {
    add_fs_writer : check(add_fs_writer),
    create_hook   : check(create_hook),
  };

  function create_hook() {
    return _over;
  }

  function add_fs_writer(w) {
    w.on('close', _over);
    w.on('error', _over);
  }

  function _over(e) {
    if (e) {
      console.log(e.message)
    }
    if (--wait == 0) {
      end = true;
      cb()
    }
  }

  function check(fn) {
    return function() {
      if (end)
        throw new Error('is end');
      ++wait;
      return fn.apply(null, arguments);
    }
  }
}
