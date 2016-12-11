module.exports = require('./test-base.js')(tfs_main);


function tfs_main(driver) {
  var fs, hdid;
  var hdfs        = require('fs');
  var gid_uid     = parseInt(10 * Math.random()) + 1;
  var mode        = parseInt(0x777 * Math.random())+1;
  var now         = Date.now();
  var dir2remove_a= false;
  var append_buf  = Math.random() > 0.5 ? '-' : '_';

  var filecontent = new Buffer(4096*3 + 1);

  try {
    hdid = hdfs.readFileSync(__dirname + '/driver-id', {encoding:'UTF-8'});
    hdid = hdid.trim();
  } catch(e) {
    throw new Error('Run "tdriver.js" first.');
  }

  for (var i=0,e=filecontent.length; i<e; ++i) {
    filecontent[i] = parseInt(Math.random() * 255);
  }

  function arrhas(arr, item) {
    for (var i=0,e=arr.length; i<e; ++i) {
      if (arr[i] == item) return true;
    }
    return false;
  }


  return {
    note : 'FS base',

    open_fs: function(test) {
      driver.open_fs(hdid, function(err, _fs) {
        fs = _fs;
        test.assert(err);
        test.assert(_fs != null, 'cannot get fs');
        test.finish();
      });
    },


    watch1: function(test) {
      test.wait('open_fs');
      var w1 = fs.watch('/dir2', { recursive: true,
          persistent:true }, function(type, fname) {
        test.log(type, fname);
      });
      w1.on('error', function(err) {
        test.assert(err);
      });
      var w2 = fs.watch('/dir1/need-delete', function(type, fname) {
        test.log(type, fname);
      });
      test.finish();
    },


    mkdir1 : function(test) {
      test.wait('open_fs', 'watch1');
      fs.mkdir('/dir1', function(err) {
        if (err && err.code != 'EEXIST') test.assert(err);
        test.finish();
      });
    },


    mkdir2 : function(test) {
      test.wait('mkdir1')
      fs.mkdir('/dir2', function(err) {
        if (err && err.code != 'EEXIST') test.assert(err);
        test.finish();
      });
    },


    mkdir2a : function(test) {
      test.wait('mkdir2')
      fs.mkdir('/dir2/a', function(err) {
        if (err && err.code != 'EEXIST') test.assert(err);
        test.finish();
      });
    },


    mkdir3 : function(test) {
      test.wait('mkdir2')
      fs.mkdir('/dir1/a', function(err) {
        if (err && err.code != 'EEXIST') test.assert(err);
        test.finish();
      });
    },


    mkdir4 : function(test) {
      test.wait('mkdir3');
      fs.mkdir('/dir1/b', function(err) {
        if (err && err.code != 'EEXIST') test.assert(err);
        test.finish();
      });
    },


    list1: function(test) {
      test.wait('mkdir4');
      fs.readdir('/', function(err, list) {
        test.assert(err);
        test.assert(arrhas(list, 'dir1'), list);
        test.assert(arrhas(list, 'dir2'), list);
        test.finish();
      });
    },


    list2: function(test) {
      test.wait('mkdir4');
      fs.readdir('/dir1', function(err, list) {
        test.assert(err);
        test.assert(arrhas(list, 'a'), list);
        test.assert(arrhas(list, 'b'), list);
        test.finish();
      });
    },


    list3: function(test) {
      test.wait('mkdir2a');
      fs.readdir('/dir2', function(err, list) {
        test.assert(err);
        if (dir2remove_a) {
          list.forEach(function(f) {
            test.assert(f != 'a', '/dir2/a  not remove');
          });
        }
        test.finish();
      });
    },


    rm_dir_fail: function(test) {
      test.wait('list2', 'mkdir2a');
      fs.rmdir('/dir2', function(err) {
        test.assert(err != null, '非空文件夹不应该被删除');
        test.finish();
      });
    },


    rm_dir: function(test) {
      test.wait('list2', 'mkdir2a', 'rm_dir_fail');
      fs.rmdir('/dir2/a', function(err) {
        test.assert(err);
        dir2remove_a = true;
        test.retest('list3');
        test.finish();
      });
    },


    'update time': function(test) {
      test.wait('mkdir1');
      fs.utimes('/dir1', now, now, function(err) {
        // test.log(now);
        test.assert(err);
        test.finish();
      });
    },


    'change-owner': function(test) {
      test.wait('mkdir1');
      fs.chown('/dir1', gid_uid, gid_uid, function(err) {
        // test.log(gid_uid);
        test.assert(err);
        test.finish();
      });
    },


    'change-mode' : function(test) {
      test.wait('mkdir1');
      fs.chmod('/dir1', mode, function(err) {
        // test.log(mode);
        test.assert(err);
        test.finish();
      });
    },


    'link-state' : function(test) {
      test.wait('mkdir1', 'update time', 'change-owner', 'change-mode');
      fs.lstat('/dir1', function(err, stat) {
        test.assert(err);
        if (stat) {
          test.assert(stat.atime.getTime() == now, 'atime not change');
          test.assert(stat.mtime.getTime() == now, 'mtime not change');
          test.assert(stat.mode  == mode, 'mode not change');
          test.assert(stat.gid == gid_uid, 'gid not change');
          test.assert(stat.uid == gid_uid, 'uid not change');
          //test.log('/dir1', stat);
        }
        test.finish();
      });
    },


    'write-file': function(test) {
      test.wait('mkdir2');
      fs.writeFile('/dir2/t.txt', filecontent, function(err, size, buffer) {
        test.assert(buffer != null, 'cannot get buffer:');
        test.assert(size == buffer.length,
          'cannot write file size: ' + size + '->' + buffer.length);
        test.assert(err);
        test.finish();
      });
    },


    'mkdir-for-rename': function(test) {
      test.wait('write-file');
      fs.unlink('/rename-target/t.doc', function() {
        fs.rmdir('/rename-target', function() {
          fs.mkdir('/rename-target', function(err) {
            //test.assert(err);
            test.finish();
          });
        })
      });
    },


    'rename' : function(test) {
      test.wait('mkdir-for-rename');
      fs.rename('/dir2/t.txt', '/rename-target/t.doc', function(err) {
        test.assert(err);
        test.finish();
      });
    },


    'read-file': function(test) {
      test.wait('rename');
      fs.readFile('/rename-target/t.doc', function(err, buffer, size) {
        if (!err) {
          var showLen = 100;
          test.assert(size == buffer.length, 'size fail1 ' + buffer.length);
          test.assert(size == filecontent.length, 'size fail2 ' + filecontent.length);
          for (var i=0; i<size; ++i) {
            if (buffer[i] != filecontent[i]) {
              test.assert(false, 'file content fail at ' + i + '['+size+']');
              test.log('src:', filecontent.slice(i-showLen, i+showLen));
              test.log('fs: ', buffer.slice(i-showLen, i+showLen));
              break;
            }
          }
        }
        test.assert(err);
        test.finish();
      });
    },


    'append' : function(test) {
      test.wait('mkdir2');
      fs.appendFile('/dir2/append.txt', append_buf, function(err) {
        test.assert(err);
        test.finish();
      });
    },


    'read-append' : function(test) {
      test.wait('append');
      fs.readFile('/dir2/append.txt', function(err, size, buffer) {
        if (buffer) {
          // test.log(size, buffer.slice(size-5, size).toString());
          test.assert(buffer[buffer.length-1] == append_buf[0]);
        }
        test.assert(err);
        test.finish();
      });
    },


    'make-deled-txt' : function(test) {
      test.wait('mkdir1');
      fs.writeFile('/dir1/need-delete', 'delete', function(err, size, buffer) {
        test.assert(err);
        test.finish();
      });
    },


    'remove-txt' : function(test) {
      test.wait('make-deled-txt');
      fs.unlink('/dir1/need-delete', function(err) {
        test.assert(err);
        test.finish();
      });
    },


    quit: function(test) {
      test.wait('change-mode', 'update time', 'list1', 'list2', 'read-append',
        'rm_dir', 'change-owner', 'link-state', 'write-file', 'read-file',
        'remove-txt', 'watch1');

      fs.quit(function(err) {
        test.finish();
      });
    },

  };
}
