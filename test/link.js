require('./test-base.js')(main);


function main(driver) {
  var fs, hdid;
  var hdfs        = require('fs');
  var now         = Date.now();
  var text        = 'test';
  var filecontent = hdfs.readFileSync('d:\\ImageDB.ddf');

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
      test.wait('cdir');
      fs.symlink('/testlink/file.txt', '/testlink/slink1', cb(test));
    },

    slink2: function(test) {
      test.wait('cdir');
      fs.symlink('/testlink/dir', '/testlink/slink2', cb(test));
    },

    slink3: function(test) {
      test.wait('slink1');
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
      test.wait('slink1');
      fs.readFile('/testlink/slink1', function(err, len, buff) {
        test.assert(buff && (buff.toString() == text), 'content fail.');
        test.assert(err);
        test.finish();
      });
    },

    quit: function(test) {
      test.wait('slink3', 'slink1', 'slink2', 'listdir', 'readfile');
      fs.quit(function(err) {
        test.finish();
      });
    },
  };
}
