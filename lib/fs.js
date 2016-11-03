var tool  = require('./tool.js');
var cqld  = require('./cql.js');
var fdp   = require('./fd.js');
var cstat = require('./stats.js');
var uuid  = require('uuid');
var os    = require('os');
var plib  = require('path').win32;
var util  = require('util');
var logger= require('logger-lib')('fs-cass');


module.exports = fs;


//
// https://nodejs.org/dist/latest-v0.12.x/docs/api/fs.html
// http://nodeapi.ucdok.com/#/api/fs.html
//
function fs(client, hdid, return_fs_cb) {
  var ret = {
    rename          : rename,
    renameSync      : none,
    ftruncate       : ftruncate,
    ftruncateSync   : none,
    truncate        : truncate,
    truncateSync    : none,
    chown           : chown,
    chownSync       : none,
    fchown          : fchown,
    fchownSync      : none,
    lchown          : lchown,
    lchownSync      : none,
    chmod           : chmod,
    chmodSync       : none,
    fchmod          : fchmod,
    fchmodSync      : none,
    lchmod          : lchmod,
    lchmodSync      : none,
    stat            : stat,
    lstat           : lstat,
    fstat           : fstat,
    statSync        : none,
    lstatSync       : none,
    fstatSync       : none,
    link            : link,
    linkSync        : none,
    symlink         : symlink,
    symlinkSync     : none,
    readlink        : readlink,
    readlinkSync    : none,
    realpath        : realpath,
    realpathSync    : none,
    unlink          : unlink,
    unlinkSync      : none,
    rmdir           : rmdir,
    rmdirSync       : none,
    mkdir           : mkdir,
    mkdirSync       : none,
    readdir         : readdir,
    readdirSync     : none,
    close           : close,
    closeSync       : none,
    open            : open,
    openSync        : none,
    utimes          : utimes,
    utimesSync      : none,
    futimes         : futimes,
    futimesSync     : none,
    fsync           : fsync,
    fsyncSync       : none,
    write           : write,
    writeSync       : none,
    read            : read,
    readSync        : none,
    readFile        : readFile,
    readFileSync    : none,
    writeFile       : writeFile,
    writeFileSync   : none,
    appendFile      : appendFile,
    appendFileSync  : none,
    watchFile       : watchFile,
    unwatchFile     : unwatchFile,
    watch           : watch,
    exists          : exists,
    existsSync      : none,
    access          : access,
    accessSync      : none,
    createReadStream  : createReadStream,
    createWriteStream : createWriteStream,

    quit            : quit,
  };

  var client_info = [os.type(), os.release(),
      os.platform(), os.arch(), uuid.v1()].join('-');

  var cqlg = cqld.gen_fs_cql(hdid, client_info);
  var def_mode = 438; // 0666
  var fd_pool;
  var root_node_id; // 根目录的 nod_id==pth_id
  var note;


return init();


  function none() {
    throw new Error('unsupport synchronous function');
  }


  function quit(cb) {
    client.exa(cqlg.sub_client_ref(), cb);
    client  = null;
    cqlg    = null;
    fd_pool = null;
  }


  function init() {
    client.exa(cqlg.add_client_ref());

    client.exa(cqld.state_drv(hdid).main, function(err, inf) {
      if (err) return return_fs_cb(err);
      if (inf.rows.length <= 0) {
        return return_fs_cb(new FileError('driver not found', hdid));
      }
      root_node_id  = inf.rows[0].root;
      note          = inf.rows[0].note;
      fd_pool       = fdp();
      return_fs_cb(null, ret);
    });
  }


  //
  // 如果是根路径返回空数组
  // 如果 参数无效抛出异常
  //
  function parsePathToArr(p) {
    var s1 = plib.normalize(p).split(plib.sep);
    var ret = [];
    for (var i=0, e=s1.length; i<e; ++i) {
      if (s1[i] && (s1[i] != '.') && (s1[i] != '..')) {
        ret.push(s1[i]);
      }
    }
    return ret;
  }


  function _real_path(p) {
    return plib.sep + parsePathToArr(p).join(plib.sep);
  }


  //
  // 通过文件路径映射找到对应节点, 一次查询即可完成
  // 但不支持权限
  //
  function pathToNodeidQuick(syscall, p, _notuse, cb) {
    return pathToNodeid(syscall, p, _notuse, cb);
    // !!!! 未完成

    // var rpath = _real_path(p);
    // if (_notuse) throw new Error('_notuse must null');
    //
    // client.exa(cqlg.find_node_with_path(rpath), function(err, inf) {
    //   if (err) return cb(
    //     new FileError(err.message, p, 'ENOENT', syscall, err.code));
    //
    //   var node = inf.rows[0];
    //   if (!node) return cb(
    //     new FileError('The path not exists', p, 'ENOENT', syscall));
    //
    //   cb(null, {
    //     node : node
    //   });
    // });
  }


  //
  // 通过文件路径遍历所有节点获取最终节点
  // syscall -- 函数名
  // offset -- 遍历到 p.length - offset
  // cb => (err, node); err 对象可以直接返回给客户
  //
  function pathToNodeid(syscall, p, offset, cb) {
    var parr  = parsePathToArr(p)
    var i     = -1;
    var len   = parr.length + offset;
    var node, pathobj;

    find_path(root_node_id);

    function find_node(_nod_id) {
      client.exa(cqlg.find_node(_nod_id), function(err, inf) {
        if (err) return cb(
          new FileError(err.message, p, 'ENOENT', syscall, err.code));
        node = inf.rows[0];
        if (!node) return cb(
          new FileError('The node not exists', p, 'ENOENT', syscall));
        find_name(len, cqld.T_DIR);
      });
    }

    function find_path(_pth_id) {
      client.exa(cqlg.find_path(_pth_id), function(err, inf) {
        if (err) return cb(
          new FileError(err.message, p, 'ENOENT', syscall, err.code));
        pathobj = inf.rows[0];
        if (!pathobj) return cb(
          new FileError('The path not exists', p, 'ENOENT', syscall));
        find_node(pathobj.nod_id);
      });
    }

    function find_name(end, target_type) {
      if (++i < end) {
        // console.log('---', node, '@@@', pathobj);
        // console.log(i, len, parr, parr[i]);
        if (i+1 < end && pathobj.type != target_type) {
          return cb(new FileError('is not '
            + cqld.type_name[target_type], p, 'ENOENT', syscall));
        }

        var name = parr[i];
        if ( pathobj.child && pathobj.child[name]) {
          return find_path(pathobj.child[name]);
        }
        cb(new FileError('Cannot find ' + name + ' in', p, 'ENOENT', syscall));

      } else {
        cb(null, {
          node : node,
          path : pathobj,
          last : parr[parr.length-1],
          next : _next,
          path_arr : parr,
        });
        // ! 小心调用
        function _next(type) {
          if (!type) throw new Error('must type ' + type);
          --i;
          find_name(end+1);
        }
      }
    }
  }


  //
  // 获取或创建 type 类型节点, throw_if_exist=true 则存在时抛出异常
  // 创建与获取返回值不同
  //
  function _create_node(syscall, path, _mode, throw_if_exist, type, cb) {
    var first_call = true;
    pathToNodeid(syscall, path, -1, function(err, inf) {
      if (err) return cb(err);

      if (first_call) {
        first_call = false;
        if (inf.path.child && inf.path.child[inf.last]) {
          if (throw_if_exist) {
            return cb(new FileError('is exists', path, 'EEXIST', syscall));
          }
          inf.next(type);

        } else {
          var _c = cqlg.create_node(inf.path.id, inf.last, _mode, _real_path(path), type);
          client.exax(_c, function(err, r) {
            if (err) return cb(
                new FileError('fail', path, 'EPERM', syscall, err.code));
            cb(null, {
              node : _c.block,
            });
          });
        }
      } else {
        cb(null, inf);
      }
    });
  }


  function delelte_nod(id) {
    var c = cqlg.delete_nod(id);
    client.exa(c.check, function(err, inf) {
      if (err) {
        logger.error(err);
      } else if (inf.rowLength == 1 && inf.rows[0].ref_pth.length <= 0) {
        client.exa(c.delete, function(err) {
          if (err)
            logger.error(err);
        });
      }
    });
  }


  function _read_block(filename, bid, fpos, flen, blocksz, tsize, cb) {
    if (fpos < 0 || flen <= 0) return cb(
      new FileError('bad position', filename, 'EFAULT', 'read'), 0);
    if (flen + fpos > tsize) flen = tsize - fpos;

    var begpos = fpos;
    var begblk = parseInt(fpos / blocksz);
    var endblk = parseInt((fpos + flen) / blocksz) + 1;
    var buffer = new Buffer(flen);
    next_blk();

    function next_blk() {
      if (begblk < endblk) {
        client.exa(cqlg.read_block(bid, begblk),
          function(err, inf) {
            if (err) return cb(
              new FileError(err.message, filename, 'EIO', 'read'),
              begpos - fpos, buffer);

            if (inf.rowLength == 1) {
              inf.rows[0].block.copy(buffer, begpos);
            } else {
              return cb(
                new FileError(err.message, filename, 'EIO', 'read'),
                begpos - fpos, buffer);
            }

            ++begblk;
            fpos += blocksz;
            next_blk();
          });
      } else {
        cb(null, begpos - fpos, buffer);
      }
    }
  }


  //
  // 总是把 buffer 完全写入
  // filename 只是用来标注出错文件
  //
  function _write_block(filename, bid, fpos, blocksz, buffer, cb) {
    var begpos = fpos;
    var begblk = parseInt(fpos / blocksz);
    var endblk = parseInt(buffer.length / blocksz) + 1;
    next_blk();

    function next_blk() {
      if (begblk < endblk) {
        var wbuf = buffer.slice(fpos, fpos + blocksz);
        client.exa(cqlg.write_block(bid, begblk, buffer),
          function(err, inf) {
            if (err) return cb(
              new FileError(err.message, filename, 'EIO', 'write'),
              begpos - fpos, buffer);

            ++begblk;
            fpos += blocksz;
            next_blk();
          });
      } else {
        write_size();
      }
    }

    function write_size() {
      client.exa(cqlg.update_size(bid, buffer.length), function(err) {
        if (err) return cb(
          new FileError(err.message, filename, 'EIO', 'write'),
          begpos - fpos, buffer);

        cb(null, begpos - fpos, buffer);
      });
    }
  }


  function rename(oldPath, newPath, callback) {}

  // ftruncate()会将参数fd指定的文件大小改为参数length指定的大小。
  // 如果原来的文件大小比参数length大，则超过的部分会被删去。
  function ftruncate(fd, len, callback) {}

  function truncate(path, len, callback) {}


  //
  //  chown将指定文件的拥有者改为指定的用户或组
  //
  function chown(path, uid, gid, callback) {
    checknul(path, callback, uid, gid);
    pathToNodeid('chown', path, 0, function(err, inf) {
      if (err) return callback(err);

      var c = cqlg.change_owner(inf.node.id, uid, gid);
      client.exax(c, make_cass_cb(callback, path, 'chown'));
    });
  }


  function fchown(fd, uid, gid, callback) {}

  // 若 path 是一个符号链接时（symbolic link）,读取的是该符号链接本身，而不是它所 链接到的文件
  function lchown(path, uid, gid, callback) {}


  //
  // 修改文件权限
  //
  function chmod(path, mode, callback) {
    checknul(path, mode, callback);
    pathToNodeid('chmod', path, 0, function(err, inf) {
      if (err) return callback(err);

      var c = cqlg.change_mode(inf.node.id, mode);
      client.exax(c, make_cass_cb(callback, path, 'chmod'));
    });
  }


  function fchmod(fd, mode, callback) {}

  // 若 path 是一个符号链接时（symbolic link）,读取的是该符号链接本身，而不是它所 链接到的文件
  function lchmod(path, mode, callback) {}

  // callback:Function(err, stats) 其中 stats 是一个 fs.Stats 对象。 详情请参考 fs.Stats
  function stat(path, callback) {}


  //
  // 若 path 是一个符号链接时（symbolic link）,
  // 读取的是该符号链接本身，而不是它所 链接到的文件
  //
  function lstat(path, callback) {
    checknul(path, callback);
    pathToNodeid('lstat', path, 0, function(err, inf) {
      if (err) return callback(err);
      callback(null, cstat(inf, hdid));
    });
  }


  function fstat(fd, callback) {}

  // creates a new link (hard link) to an existing file
  function link(srcpath, dstpath, callback) {}

  // 与link 相同
  function symlink(srcpath, dstpath, _type, callback) {}

  // 返回符号链接指向的文件名, callback:(err, linkString)
  function readlink(path, callback) {}


  //
  // process.cwd 可能定位到相对路径, 返回完整路径
  //
  function realpath(path, _cache, callback) {
    checknul(path, callback);
    if (!callback) {
      callback = _cache;
    }
    if (_cache && _cache[path]) {
      callback(null, _cache[path]);
    } else {
      callback(null, path);
    }
  }

  //
  // deletes a name from the file system
  //
  function unlink(path, callback) {}


  function rmdir(path, callback) {
    checknul(path, callback);
    pathToNodeid('rmdir', path, 0, function(err, inf) {
      if (err) return callback(err);

      if (inf.path.child) {
        return callback(new FileError(
          'Directory not empty', path, 'ENOTEMPTY', 'rmdir'));
      }
      client.exax(cqlg.remove_dir(inf),
          make_cass_cb(callback, path, 'rmdir'));

      delelte_nod(inf.path.nod_id);
    });
  }


  function mkdir(path, _mode, callback) {
    if (!callback) {
      callback = _mode;
      _mode = def_mode;
    }
    checknul(path, callback);
    _create_node('mkdir', path, _mode, true, cqld.T_DIR, callback);
  }


  function readdir(path, callback) {
    checknul(path, callback);
    pathToNodeid('readdir', path, 0, function(err, inf) {
      if (err) return callback(err);
      var list = [];
      for (var n in inf.path.child) {
        list.push(n);
      }
      callback(null, list);
    });
  }


  function close(fd, callback) {}

  function open(path, flags, _mode, callback) {}


  //
  // 更改 path 所指向的文件的时间戳。access time, modification time
  //
  function utimes(path, atime, mtime, callback) {
    checknul(path, callback, atime, mtime);
    pathToNodeid('utime', path, 0, function(err, inf) {
      if (err) return callback(err);

      var c = cqlg.update_time(inf.node.id, atime, mtime);
      client.exax(c, make_cass_cb(callback, path, 'utime'));
    });
  }


  function futimes(fd, atime, mtime, callback) {}

  // 使文件的 modification time 设置为当前系统时间
  function fsync(fd, callback) {}

  // (fd, data[, position[, encoding]], callback)
  function write(fd, buffer, offset, length, _position, callback) {}

  function read(fd, buffer, offset, length, position, callback) {}


  function readFile(filename, _options, callback) {
    if (!callback) {
      callback = _options;
      _options = {};
    }
    checknul(filename, callback);

    var encoding = _options.encoding || 'utf8',
        flag     = parse_open_flag(_options.flag || 'r');

    if (!flag.read) {
      return callback(
        new FileError('flag fail', filename, 'EINVAL', 'read'), 0);
    }

    pathToNodeid('read', filename, 0, function(err, inf) {
      if (err) return callback(err);
      var node = inf.node;

      _read_block(filename, node.id, 0,
        node.blocksz, node.blocksz, node.tsize, callback);
    });
  }


  function writeFile(filename, data, _options, callback) {
    if (!callback) {
      callback = _options;
      _options = {};
    }
    checknul(filename, data, callback);

    var encoding = _options.encoding || 'utf8',
        _mode    = _options.mode || def_mode,
        flag     = parse_open_flag(_options.flag || 'w');

    if (!util.isBuffer(data)) {
      data = new Buffer(data, encoding);
    }
    if (!flag.write) {
      return callback(
        new FileError('flag fail', filename, 'EINVAL', 'write'), 0, data);
    }

    _create_node('write', filename, _mode, flag.throw_if_exist, cqld.T_FILE,
      function(err, inf) {
        if (err) return callback(err, 0, data);
        _write_block(filename, inf.node.id, 0, inf.node.blocksz, data, callback);
      });
  }


  function appendFile(filename, data, _options, callback) {
    if (!callback) {
      callback = _options;
      _options = {};
    }
    checknul(filename, data, callback);

    var encoding = _options.encoding || 'utf8',
        _mode    = _options.mode || def_mode,
        flag     = parse_open_flag(_options.flag || 'a');

    if (!util.isBuffer(data)) {
      data = new Buffer(data, encoding);
    }
    if (!flag.append) {
      return callback(
        new FileError('flag fail', filename, 'EINVAL', 'write'), 0, data);
    }
    callback(new Error('to be continue..'));
    // read buffer, and append ...
  }


  function watchFile(filename, _options, listener) {}

  function unwatchFile(filename, _listener) {}

  function watch(filename, _options, _listener) {}

  function exists(path, callback) {}

  function access(path, _mode, callback) {}

  function createReadStream(path, _options) {}

  function createWriteStream(path, _options) {}
}


//  errno: -4058, code: 'ENOENT', syscall: 'mkdir', path: 'x:\\a'
function FileError(msg, path, code, syscall, errno) {
  if (path) msg = msg + ': `' + path + '`';
  this.path     = path;
  this.code     = code    || 'UNKNOW';
  this.errno    = errno   || '-1';
  this.syscall  = syscall || 'UNKNOW';
  this.message  = msg;
  this.stack    = new Error().stack;
}

util.inherits(FileError, Error);


function checknul() {
  for (var i=0, e=arguments.length; i<e; ++i) {
    if ((!arguments[i]) && isNaN(arguments[i]) ) {
      throw new Error('null exception ' + i);
    }
  }
}


function make_cass_cb(cb, path, syscall, _return_info) {
  return function(err, info) {
    err && console.log('!!! make_cass_cb', err) ; // !!!!
    if (err)
      cb(new FileError('fail', path, 'EPERM', syscall, err.code));
    if (_return_info)
      cb(null, info);
    else
      cb();
  }
}


function parse_open_flag(f) {
  var ret = {
    read                : false,
    write               : false,
    append              : false,
    throw_if_exist      : false,
  };

  for (var i=0, e=f.length; i<e; ++i) {
    var c = f[i];
    switch (c) {
      case 'r':
        ret.read = true;
        break;
      case '+':
        ret.read = ret.write = true;
        break;
      case 'x':
        ret.throw_if_exist = true;
        break;
      case 'w':
        ret.write = true;
        break;
      case 'a':
        ret.append = true;
        break;
    }
  }

  return ret;
}
