var fs_cass   = require('../');
var cassandra = require('cassandra-driver');
var conlib    = require('configuration-lib');
var client;
var driver;

var hdid = '91ed0cf0-9db2-11e6-9b97-e1bc8e58dfd4';


conlib.wait_init(function() {
  var conf = conlib.load();

  client = new cassandra.Client(conf.cassandra);
  driver = fs_cass.open_driver(client);

  //tdriver();
  tfs();
});


function tdriver() {
  tcreatehd('test1', function(info) {
    tlisthd(function(list) {
      tstatehd(list[0], function() {
          //tdeletehd(info.hd_id, function() {
            //tdeletehd('909090')
            end();
          //});
      });
    });
  });
}


function tfs() {
  tfs_open(hdid, function(fs) {
    tfs_quit(fs);
  });
}


function tfs_quit(fs, next) {
  fs.quit(function(err) {
    end(err, 'close fs', next);
  });
}


function tfs_open(id, next) {
  driver.open_fs(id, function(err, fs) {
    end(err, 'open fs', next, fs);
  });
}

function tdeletehd(id, next) {
  driver.delete(id, function(err) {
    end(err, 'delete hd', next, id);
  });
}


function tcreatehd(desc, next) {
  driver.create('test1', function(err, info) {
    end(err, 'create hd', next, info);
  });
}


function tlisthd(next) {
  driver.list(function(err, list) {
    end(err, 'list hd', next, list);
  });
}


function tstatehd(id, next) {
  driver.state(id, function(err, info) {
    end(err, 'state hd', next, info);
  });
}


function end(err, name, next, parms) {
  if (err) {
    console.log('\nfail', name, err);
  } else {
    console.log('\nsuccess', name);
    if (next)
      return next(parms);
  }
  process.exit();
}
