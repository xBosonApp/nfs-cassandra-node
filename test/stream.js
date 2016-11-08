module.exports = require('./test-base.js')(main);


function main(driver) {
  var fs, hdid, fd1;
  var hdfs        = require('fs');
  var gid_uid     = parseInt(10 * Math.random()) + 1;
  var mode        = parseInt(0x777 * Math.random())+1;
  var now         = Date.now();
  var dir2remove_a= false;
  var append_buf  = Math.random() > 0.5 ? '-' : '_';
  var filecontent = new Buffer(4096*3 + 1);

  try {
    hdid = hdfs.readFileSync(__dirname + '/driver-id', {encoding:'UTF-8'});
  } catch(e) {
    throw new Error('Run "tdriver.js" first.');
  }

  for (var i=0,e=filecontent.length; i<e; ++i) {
    filecontent[i] = parseInt(Math.random() * 255);
  }

  var streamopt = {start: 4096+1, flags: 'rw'};

  return {
    note: 'Stream',

    open_fs: function(test) {
      driver.open_fs(hdid, function(err, _fs) {
        fs = _fs;
        test.assert(err);
        test.assert(_fs != null, 'cannot get fs');
        test.finish();
      });
    },

    writer: function(test) {
      test.wait('open_fs');
      var writer = fs.createWriteStream('/rename-target/stream.ini', streamopt);
      writer.end(filecontent, function() {
        // test.log('file length', filecontent.length);
        test.finish();
      });
    },

    reader: function(test) {
      test.wait('writer');
      var rpos = 0;
      var readable = fs.createReadStream('/rename-target/stream.ini', streamopt);
      var fail = false;

      readable.on('data', function(chunk) {
        if (fail) return;
        // if (rpos == 0) { console.log('[0]', chunk); }
        for (var i=0,e=chunk.length; i<e; ++i) {
          if (chunk[i] != filecontent[rpos+i]) {
            test.assert(false, 'content fail at ' + (i+rpos) );
            console.log('FAIL:\n  POS =', rpos + i, ' I=', i)
            console.log('  FILE=', filecontent.slice(i+rpos), filecontent.length);
            console.log('  CHUNK=', chunk.slice(i), chunk.length)
            fail = true;
            readable.emit('error', 'fail');
            break;
          }
        }
        rpos += chunk.length;
      });
      // readable.on('end', over);
      readable.on('close', over);
      function over() {
        test.assert(rpos == filecontent.length,
          'length fail.' + rpos + ':' + filecontent.length);
        test.finish();
      }
      readable.on('error', function(err) {
        test.assert(err);
      });
    },

    size: function(test) {
      test.wait('reader');
      fs.stat('/rename-target/stream.ini', function(err, stat) {
        test.assert(err);
        stat && test.assert(
            stat.size - streamopt.start == filecontent.length,
            'length fail: ' + stat.size);
        test.finish();
      });
    },

    quit: function(test) {
      test.wait('size');
      fs.quit(function(err) {
        test.finish();
      });
    },
  };
}
