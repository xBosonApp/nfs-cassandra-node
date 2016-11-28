
module.exports = function(fn) {
  var fs_cass   = require('../');
  var cassandra = require('cassandra-driver');
  var conlib    = require('configuration-lib');
  var test      = require('./ymtest.js');
  var Event     = require('events');
  var client    = global.client;
  var driver    = null;
  var eve = new Event();


  fs_cass.open_driver(function(err, driver) {
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
