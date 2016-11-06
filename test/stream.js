module.exports = require('./test-base.js')(main);


function main(driver) {
  var fs, hdid, fd1;
  var hdfs        = require('fs');
  var gid_uid     = parseInt(10 * Math.random()) + 1;
  var mode        = parseInt(0x777 * Math.random())+1;
  var now         = Date.now();
  var dir2remove_a= false;
  var append_buf  = Math.random() > 0.5 ? '-' : '_';
  var filecontent = hdfs.readFileSync('d:\\ImageDB.ddf');
  // var filecontent = new Buffer('abcdefdsajkfl;das');

  try {
    hdid = hdfs.readFileSync(__dirname + '/driver-id', {encoding:'UTF-8'});
  } catch(e) {
    throw new Error('Run "tdriver.js" first.');
  }

  return {
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
      var writer = fs.createWriteStream('/rename-target/stream.ini');
      writer.end(filecontent, function() {
        test.finish();
      });
    },

    reader: function(test) {
      test.wait('writer');
      var rpos = 0;
      var readable = fs.createReadStream('/rename-target/stream.ini');
      var fail = false;

      readable.on('data', function(chunk) {
        if (fail) return;
        for (var i=0,e=chunk.length; i<e; ++i) {
          if (chunk[i] != filecontent[rpos+i]) {
            test.assert(false, 'content fail at ' + (i+rpos) );
            console.log('POS', rpos, ' I', i)
            console.log('fl:', filecontent.slice(i+rpos), filecontent.length);
            console.log('ch:', chunk.slice(i), chunk.length)
            fail = true;
            readable.pause();
            test.finish();
            break;
          }
        }
        rpos += chunk.length;
      });
      readable.on('end', function() {
        test.assert(rpos == filecontent.length, 'length fail.');
        test.finish();
      });
    },

    quit: function(test) {
      test.wait('reader');
      fs.quit(function(err) {
        test.finish();
      });
    },
  };
}
