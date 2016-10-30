require('./test-base.js')(tfs_main);


function tfs_main(driver) {
  var fs, hdid;

  try {
    hdid = require('fs').readFileSync(__dirname + '/driver-id', {encoding:'UTF-8'});
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

    mkdir1 : function(test) {
      if (!test.wait('open_fs')) return;
      fs.mkdir('/dir1', function(err) {
        if (err && err.code != 'EEXIST') test.assert(err);
        test.finish();
      });
    },

    mkdir2 : function(test) {
      if (!test.wait('mkdir1')) return;
      fs.mkdir('/dir2', function(err) {
        if (err && err.code != 'EEXIST') test.assert(err);
        test.finish();
      });
    },

    mkdir2a : function(test) {
      if (!test.wait('mkdir2')) return;
      fs.mkdir('/dir2/a', function(err) {
        if (err && err.code != 'EEXIST') test.assert(err);
        test.finish();
      });
    },

    mkdir3 : function(test) {
      if (!test.wait('mkdir2')) return;
      fs.mkdir('/dir1/a', function(err) {
        if (err && err.code != 'EEXIST') test.assert(err);
        test.finish();
      });
    },

    mkdir4 : function(test) {
      if (!test.wait('mkdir3')) return;
      fs.mkdir('/dir1/b', function(err) {
        if (err && err.code != 'EEXIST') test.assert(err);
        test.finish();
      });
    },

    list1: function(test) {
      if (!test.wait('mkdir4')) return;
      fs.readdir('/', function(err, list) {
        test.assert(err);
        test.log('/:', list);
        test.finish();
      });
    },

    list2: function(test) {
      if (!test.wait('mkdir4')) return;
      fs.readdir('/dir1', function(err, list) {
        test.assert(err);
        test.log('/dir1:', list);
        test.finish();
      });
    },

    list3: function(test) {
      if (!test.wait('mkdir2a')) return;
      fs.readdir('/dir2', function(err, list) {
        test.assert(err);
        test.log('/dir2:', list);
        test.finish();
      });
    },

    rm_dir_fail: function(test) {
      if (!test.wait('list2', 'mkdir2a')) return;
      fs.rmdir('/dir2', function(err) {
        test.assert(err != null, '非空文件夹不应该被删除');
        test.finish();
      });
    },

    rm_dir: function(test) {
      if (!test.wait('list2', 'mkdir2a', 'rm_dir_fail')) return;
      fs.rmdir('/dir2/a', function(err) {
        test.assert(err);
        test.retest('list3');
        test.finish();
      });
    },

    quit: function(test) {
      if (!test.wait('list1', 'list2', 'rm_dir')) return;
      fs.quit(function(err) {
        test.finish();
      });
    },
  };
}
