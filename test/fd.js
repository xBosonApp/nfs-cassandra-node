require('./test-base.js')(main);


function main(driver) {
  var fs, hdid, fd1;
  var hdfs        = require('fs');
  var gid_uid     = parseInt(10 * Math.random()) + 1;
  var mode        = parseInt(0x777 * Math.random())+1;
  var now         = Date.now();
  var dir2remove_a= false;
  var append_buf  = Math.random() > 0.5 ? '-' : '_';

  var filecontent = hdfs.readFileSync('d:\\ImageDB.ddf');

  try {
    hdid = hdfs.readFileSync(__dirname + '/driver-id', {encoding:'UTF-8'});
  } catch(e) {
    throw new Error('Run "tdriver.js" first.');
  }
  var ret;
  return ret = {
    open_fs: function(test) {
      driver.open_fs(hdid, function(err, _fs) {
        fs = _fs;
        test.assert(err);
        test.assert(_fs != null, 'cannot get fs');
        test.finish();
      });
    },

    open_fd1: function(test) {
      test.wait('open_fs');
      fs.open('/fd.txt', 'rw', function(err, fd) {
        fd1 = fd;
        test.assert(err);
        test.assert(fd != null);
        test.finish();
      });
    },

    'change-time' : function(test) {
      test.wait('open_fd1');
      fs.futimes(fd1, now, now, function(err) {
        test.assert(err);
        test.finish();
      });
    },

    'sync': function(test) {
      test.wait('open_fd1');
      fs.fsync(fd1, function(err) {
        test.assert(err);
        test.finish();
      });
    },

    buf: new Buffer('hello', 'utf8'),
    str: ' world',

    'write_buffer': function(test) {
      test.wait('open_fd1');
      var buf = this.buf;
      fs.write(fd1, buf, 0, buf.length, 0, function(err, len, b) {
        test.assert(err);
        test.assert(len == buf.length, 'length fail.');
        test.assert(b != null, 'not get buffer');
        test.finish();
      });
    },

    'write_string' : function(test) {
      test.wait('write_buffer');
      fs.write(fd1, this.str, function(err, len, b) {
        test.assert(err);
        test.assert(len > 0, 'length fail.');
        test.assert(b != null, 'not get buffer');
        test.finish();
      });
    },

    'read' : function(test) {
      test.wait('write_string');
      var buf = new Buffer(this.buf.length + this.str.length);
      fs.read(fd1, buf, 0, buf.length, 0, function(err, rlen, buf) {
        test.assert(err);
        test.assert(rlen == buf.length, 'length fail.');
        if (buf) {
          test.assert(buf+'' == ret.buf+ret.str, 'content fail');
        }
        test.finish();
      });
    },

    close_fd1: function(test) {
      test.wait('open_fd1', 'change-time', 'write_buffer', 'read');
      fs.close(fd1, function(err) {
        test.assert(err);
        test.finish();
      });
    },

    quit: function(test) {
      test.wait('close_fd1');
      fs.quit(function(err) {
        test.finish();
      });
    },
  };
}
