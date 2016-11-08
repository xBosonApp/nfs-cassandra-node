module.exports = require('./test-base.js')(main);


function main(driver) {
  var fs, hdid;
  var hdfs        = require('fs');
  var now         = Date.now();
  var text        = 'test';
  var filecontent = hdfs.readFileSync('d:\\ImageDB.ddf');
  var mode        = parseInt(0x777 * Math.random())+1;
  var gid_uid     = parseInt(10 * Math.random()) + 1;

  try {
    hdid = hdfs.readFileSync(__dirname + '/driver-id', {encoding:'UTF-8'});
  } catch(e) {
    throw new Error('Run "tdriver.js" first.');
  }

  /*
    /testlink
      file.txt,
      dir,
      hlink, to file.txt
      slink1, to file.txt
      slink2, to dir
      slink3, to slink1
  */
  function cb(test) {
    return function(err) {
      if (err && err.code != 'EEXIST')
        test.assert(err);
      test.finish();
    }
  }

  return {
    note: 'LINK',
    
    open_fs: function(test) {
      driver.open_fs(hdid, function(err, _fs) {
        fs = _fs;
        test.assert(err);
        test.assert(_fs != null, 'cannot get fs');
        test.finish();
      });
    },

    mkdir: function(test) {
      test.wait('open_fs');
      fs.mkdir('/testlink', cb(test));
    },

    cfile: function(test) {
      test.wait('mkdir');
      fs.writeFile('/testlink/file.txt', text, cb(test));
    },

    cdir: function(test) {
      test.wait('cfile');
      fs.mkdir('/testlink/dir', cb(test));
    },

    hlink: function(test) {
      test.wait('cdir');
      fs.link('/testlink/file.txt', '/testlink/hlink', cb(test));
    },

    slink1: function(test) {
      test.wait('hlink');
      fs.symlink('/testlink/file.txt', '/testlink/slink1', cb(test));
    },

    slink2: function(test) {
      test.wait('slink1');
      fs.symlink('/testlink/dir', '/testlink/slink2', cb(test));
    },

    slink3: function(test) {
      test.wait('slink2');
      fs.symlink('/testlink/slink1', '/testlink/slink3', cb(test));
    },

    cdirs: function(test) {
      test.wait('slink3');
      fs.mkdir('/testlink/dir/a', function(err) {
        ///test.assert(err);
        fs.mkdir('/testlink/dir/b', function(err) {
          //test.assert(err);
          test.finish();
        });
      });
    },

    listdir: function(test) {
      test.wait('cdirs');
      fs.readdir('/testlink/slink2', function(err, list) {
        test.assert(err);
        test.assert(list[0] == 'a', 'dir fail 0');
        test.assert(list[1] == 'b', 'dir fail 1');
        test.finish();
      });
    },

    readfile: function(test) {
      test.wait('listdir');
      fs.readFile('/testlink/slink1', function(err, len, buff) {
        test.assert(buff && (buff.toString() == text), 'content fail.');
        test.assert(err);
        test.finish();
      });
    },

    chmod1: function(test) {
      test.wait('readfile');
      fs.chmod('/testlink/slink1', mode, function(err) {
        // test.log('file mode', mode);
        test.assert(err);
        test.finish();
      });
    },

    chmod2: function(test) {
      test.wait('chmod1');
      fs.lchmod('/testlink/slink1', mode+1, function(err) {
        // test.log('link mode', mode+1);
        test.assert(err);
        test.finish();
      });
    },

    chown1: function(test) {
      test.wait('chmod1');
      fs.chown('/testlink/slink1', gid_uid, gid_uid, function(err) {
        test.assert(err);
        test.finish();
      });
    },

    chown2: function(test) {
      test.wait('chown1');
      fs.lchown('/testlink/slink1', gid_uid, gid_uid, function(err) {
        test.assert(err);
        test.finish();
      });
    },

    truncate: function(test) {
      test.wait('chown2');
      fs.truncate('/testlink/hlink', 1, function(err) {
        test.assert(err);
        test.finish();
      });
    },

    stat1: function(test) {
      test.wait('truncate');
      fs.lstat('/testlink/slink1', function(err, stat) {
        test.assert(err);
        if (stat) {
          test.assert(stat.isSymbolicLink(), 'fail symbolic');
          test.assert(stat.mode == mode+1, 'fail mode ' + stat.mode);
          test.assert(stat.gid == gid_uid, 'fail gid');
          test.assert(stat.uid == gid_uid, 'fail gid');
        }
        test.finish();
      });
    },

    stat2: function(test) {
      test.wait('stat1');
      fs.stat('/testlink/slink1', function(err, s) {
        test.assert(err);
        if (s) {
          test.assert(s.mode == mode, 'fail mode ');
          test.assert(s.isFile(), 'is not file');
          test.assert(s.gid == gid_uid, 'fail gid');
          test.assert(s.uid == gid_uid, 'fail gid');
          test.assert(s.size == 1, 'fail size ' + s.size);
        }
        test.finish();
      });
    },

    quit: function(test) {
      test.wait('stat2');
      fs.quit(function(err) {
        test.finish();
      });
    },
  };
}
