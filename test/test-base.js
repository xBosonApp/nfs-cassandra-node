
module.exports = function(fn) {
  var fs_cass   = require('../');
  var cassandra = require('cassandra-driver');
  var conlib    = require('configuration-lib');
  var test      = require('./ymtest.js');
  var Event     = require('events');
  var client;
  var driver;
  var eve = new Event();


  conlib.wait_init(function() {
    var conf = conlib.load();

    client = new cassandra.Client(conf.cassandra);
    driver = fs_cass.open_driver(client);

    var t = test(fn(driver, client));
    t.on('finish', function() {
      if (!eve.emit('finish')) {
        process.exit();
      }
    });
    t.start();
  });

  return eve;
};
