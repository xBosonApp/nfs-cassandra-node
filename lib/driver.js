var uuid = require('uuid');
var cqld = require('./cql.js');
var tool = require('./tool.js');
var cfs  = require('./fs.js');


module.exports = {
  open_driver : open_driver,
};


function open_driver(_client) {
  if (!_client) {
    throw new Error('client is null');
  }
  tool.client_pack(_client);

  var ret = {
    create    : create,
    delete    : _delete,
    list      : list,
    state     : state,
    open_fs   : open_fs,
    close_fs  : close_fs,
  };

  return ret;


  function open_fs(hdid, cb) {
    cfs(_client, hdid, cb);
  }


  function create(desc, cb) {
    var hdid = uuid.v1();
    var cql = cqld.create_driver(hdid, desc);

    tool.do_cqls2(_client, cql, 4, 0, function(err) {
      if (err) return cb(tool.filter(err));
      _client.exa(cql, function(e1) {
        if (e1) return cb(tool.filter(e1));
        _client.exa(cql.slice(2), function(e2) {
          if (e1) return cb(tool.filter(e1));
          cb(null, { hd_id: hdid });
        });
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
