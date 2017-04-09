
module.exports = function(fn) {
  var fs_cass   = require('../');
  var cassandra = require('cassandra-driver');
  var conlib    = require('configuration-lib');
  var test      = require('./ymtest.js');
  var Event     = require('events');
  var client    = global.client;
  var driver    = null;
  var eve = new Event();

  var opt = {
    watch_impl : watch_impl(),
  };

  fs_cass.open_driver(opt, function(err, driver) {
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


function watch_impl() {
  var Event = require("events");
  
  return {
    // Function(hdid, filename, _options)
    watch  : watch,
    // Function(hdid, filename, type) 发布文件修改消息, filename 文件名, type 操作类型.
    change : change,
    // Function() 关闭监听器
    end    : end,
  };

  function end() {
    // console.log('Watcher /// end');
  }

  function watch(hdid, filename, _options) {
    // console.log('Watcher /// watch', hdid, filename, _options);
    var w = new Event();
    w.close = function() {
      w.removeAllListeners();
    };
    return w;
  }

  function change(hdid, filename, type) {
    // console.log('Watcher /// change', hdid, filename, type);
  }
}
