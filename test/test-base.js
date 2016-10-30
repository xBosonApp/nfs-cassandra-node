
module.exports = function(fn) {
  var fs_cass   = require('../');
  var cassandra = require('cassandra-driver');
  var conlib    = require('configuration-lib');
  var test      = require('./ymtest.js');
  var client;
  var driver;


  conlib.wait_init(function() {
    var conf = conlib.load();

    client = new cassandra.Client(conf.cassandra);
    driver = fs_cass.open_driver(client);

    var t = test(fn(driver, client));
    t.on('finish', function() {
      process.exit();
    });
    t.start();
  });
}
