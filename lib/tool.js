var logger = require('logger-lib')('fs-cassandra');


module.exports = {
  do_cql      : do_cql,
  do_cqls2    : do_cqls2,
  node_table  : node_table,
  block_table : block_table,
  path_table  : path_table,
  norms_name  : norms_name,
  filter      : filter,
  client_pack : client_pack,
};


//
// 给 client 附加函数.
//
function client_pack(client) {
  //
  // execute array
  // cqls[0] -- cqls
  // cqls[1] -- bind parms
  // cb      -- can null
  //
  client.exa = function(cqls, cb) {
    client.execute(cqls[0], cqls[1], { prepare: true}, function(err, inf) {
      if (cb) {
        cb(err, inf);
      } else {
        err && logger(err);
      }
    });
  };

  //
  // execute array, 任何一个出错都会失败, 返回最后一次的结果
  // cqls[0.2.4...n]   -- cqls
  // cqls[1.3.5...n+1] -- bind parms
  // cb -- not null
  //
  client.exax = function(cqls, cb) {
    var ret;
    var i = 0;
    next();

    function next() {
      if (i < cqls.length) {
        //console.log(cqls[i], '\n', cqls[i+1])
        client.execute(cqls[i], cqls[i+1], {prepare: true}, function(err, inf) {
          if (err) return cb(err);
          ret = inf;
          i += 2;
          next();
        });
      } else {
        cb(null, ret);
      }
    }
  }
}


// 表名称不能超过 48 个字符
function node_table(hdid) {
  return ' drv_nod_' + hdid + ' ';
}


function block_table(hdid) {
  return ' drv_blk_' + hdid + ' ';
}


function path_table(hdid) {
  return ' drv_pth_' + hdid + ' ';
}


function norms_name(_in) {
  var ret = [];
  var c;
  for (var i=_in.length-1; i>=0; --i) {
    c = _in[i];
    if (c == '-') c = '_';
    ret[i] = c;
  }
  return ret.join('');
}


function filter(err) {
  logger.debug(err);
  var r = new Error(err.message);
  r.code = err.code;
  r.info = err.info;
  return r;
}


function do_cql(client, cql_arr, next) {
  var i = -1;
  var ress = [];
  _do();

  function _do() {
    if (++i<cql_arr.length) {
      var c = cql_arr[i];
      console.log('\nDO: [\n', c);
      client.execute(c, [], function(err, res) {
        ress.push(res);
        if (err) {
          console.log('] fail', err);
          next(err, ress);
          return;
        }
        console.log('] ok');
        _do();
      });
    } else {
      next(null, ress);
    }
  }
}


function do_cqls2(client, cql_arr, begin, end, next) {
  var i = begin - 1;
  var ress = [];

  if (!end) end = cql_arr.length;

  _do();

  function _do() {
    if (++i<end) {
      var c = cql_arr[i];
      client.execute(c, [], function(err, res) {
        ress.push(res);
        if (err) {
          next(err, ress);
          return;
        }
        _do();
      });
    } else {
      next(null, ress);
    }
  }
}
