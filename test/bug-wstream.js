var fs_cass   = require('../');


var driverid = '937f8100-1b2b-11e7-9d3d-03b23e58f867';
var wait;


fs_cass.open_driver(function(err, driver) {
  driver.open_fs(driverid, function(err, fs) {
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


function wstream(fs, drv) {
  var w1 = one('1.txt', '1', 22);
  var w2 = one('2.txt', '2', 33);

  while (
       w2.write()
    || w1.write()
  );


  function one(filename, ch, len) {
    var w = fs.createWriteStream(filename);
    wait.add_fs_writer(w);

    var blen = 1023;

    var buf = Buffer.alloc(blen);
    for (var i=0; i<blen; ++i) {
      buf[i] = ch;
    }

    var over = wait.create_hook();

    w.on('close', function() {
      var fail;
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
          console.log('Success', filename);
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
