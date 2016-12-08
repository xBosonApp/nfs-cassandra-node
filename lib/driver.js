var uuid      = require('uuid');
var logger    = require('logger-lib')('fs-cass');
var cassandra = require('cassandra-driver');
var conlib    = require('configuration-lib');
var cache     = require('cache-redis-lib');
var cqld      = require('./cql.js');
var tool      = require('./tool.js');
var cfs       = require('./fs.js');


module.exports = {
  open_driver : open_driver,
};


function open_driver(ex_conf, _cb) {
  var _client, _rediscli;

  if (typeof ex_conf == 'function' && _cb == null) {
    _cb = ex_conf;
    ex_conf = null;
  }
  if (!_cb)
    throw new Error('must callback');
  if (ex_conf) {
    _rediscli = ex_conf.redis_client;
    _client   = ex_conf.cassandra_client;
  }

  conlib.wait_init(function() {
    var conf = conlib.load();
    try {
      if (!_rediscli)
        _rediscli = cache.createClient(conf.redis_conf);
      if (!_client)
        _client = new cassandra.Client(conf.cassandra);
      tool.client_pack(_client);
    } catch(e) {
      _client   && _client.shutdown();
      _rediscli && _rediscli.end();
      return _cb(e);
    }
    init_success();
  });


  function init_success() {
    var _driver = {
      create    : create,
      delete    : _delete,
      list      : list,
      state     : state,
      open_fs   : open_fs,
      close_fs  : close_fs,
      end       : end,
    };
    _cb(null, _driver);
  }


  function open_fs(hdid, cb) {
    cfs(_client, _rediscli, hdid, cb);
  }


  function end() {
    _client.shutdown();
    _rediscli.end();
    _client = _rediscli = null;
  }


  function create(desc, cb) {
    var hdid = uuid.v1();
    var cql = cqld.create_driver(hdid, desc);
    var splite_idx = 6;

    tool.do_cqls2(_client, cql, splite_idx, 0, function(err) {
      if (err) return cb(tool.filter(err));
      _client.exax(cql.slice(0, splite_idx), function(e1) {
        if (e1) return cb(tool.filter(e1));
        cb(null, { hd_id: hdid });
      });
    });
  }


  function _delete(id, cb) {
    var cql = cqld.delete_driver(id);
    _client.execute(cql[0], [id], function(err) {
      if (err) return cb(tool.filter(err));
      tool.do_cqls2(_client, cql, 1, 0, cb);
    });
  }


  function list(cb) {
    _client.execute(cqld.list_drv(), null, function(err, ret) {
      if (err) return cb(tool.filter(err));
      var ids = [];
      for (var i=ret.rows.length-1; i>=0; --i) {
        ids[i] = ret.rows[i].id.toString();
      }
      cb(null, ids);
    });
  }


  function state(hdid, cb) {
    var cs = cqld.state_drv(hdid);
    _client.exa(cs.main, function(err, ret) {
      if (err) return cb(tool.filter(err));
      var state = ret.rows[0];

      _client.exa(cs.ref, function(err, ret) {
        if (err) return cb(tool.filter(err));
        if (ret.rows[0]) {
          state.open_cnt = ret.rows[0].ref;
        } else {
          state.open_cnt = 0;
        }
        cb(null, state);
      });
    });
  }


  function close_fs(fsobj) {
    fsobj.quit();
  }
}
