
module.exports = function(fn) {
  var fs_cass   = require('../');
  var cassandra = require('cassandra-driver');
  var conlib    = require('configuration-lib');
  var test      = require('./ymtest.js');
  var Event     = require('events');
  var client    = global.client;
  var driver    = null;
  var eve = new Event();


  conlib.wait_init(function() {
    var conf = conlib.load();

    if (!client) {
      client = new cassandra.Client(conf.cassandra);
      global.client = client;
    }
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
