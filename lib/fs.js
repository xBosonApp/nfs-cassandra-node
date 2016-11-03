var tool  = require('./tool.js');
var cqld  = require('./cql.js');
var fdp   = require('./fd.js');
var cstat = require('./stats.js');
var uuid  = require('uuid');
var os    = require('os');
var plib  = require('path').win32;
var util  = require('util');


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
  var fd_pool;
  var root_node_id;
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
      root_node_id = inf.rows[0].root;
      note = inf.rows[0].note;
      fd_pool = fdp();
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
    var rpath = _real_path(p);
    if (_notuse) throw new Error('_notuse must null');

    client.exa(cqlg.find_node_with_path(rpath), function(err, inf) {
      if (err) return cb(
        new FileError(err.message, p, 'ENOENT', syscall, err.code));

      var node = inf.rows[0];
      if (!node) return cb(
        new FileError('The path not exists', p, 'ENOENT', syscall));

      cb(null, {
        node : node
      });
    });
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
    var node;

    find_node(root_node_id);

    function find_node(_id, _type) {
      client.exa(cqlg.find_node(_id), function(err, inf) {
        if (err) return cb(
          new FileError(err.message, p, 'ENOENT', syscall, err.code));
        node = inf.rows[0];
        if (!node) return cb(
          new FileError('The path not exists', p, 'ENOENT', syscall));
        find_name();
      });
    }

    function find_name() {
      // console.log(i, len, parr, node);
      if (++i < len) {
        if (i+1 < len && node.type != cqld.T_DIR) {
          return cb(new FileError('is not directory', p, 'ENOENT', syscall));
        }

        var name = parr[i];
        var _id = node.child[name];

        if (_id) {
          find_node(_id);
        } else {
          cb(new FileError('Cannot find ' + name + ' in', p, 'ENOENT', syscall));
        }
      } else {
        cb(null, {
          node : node,
          last : parr[parr.length-1],
          path_arr : parr,
        });
      }
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
    pathToNodeidQuick('chown', path, 0, function(err, inf) {
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
    pathToNodeidQuick('chmod', path, 0, function(err, inf) {
      if (err) return callback(err);

      var c = cqlg.change_own(inf.node.id, mode);
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
    pathToNodeidQuick('lstat', path, 0, function(err, inf) {
      if (err) return callback(err);
      callback(null, cstat(inf.node, hdid));
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
    pathToNodeidQuick('rmdir', path, 0, function(err, inf) {
      if (err) return callback(err);

      if (inf.node.child) {
        return callback(new FileError(
          'Directory not empty', path, 'ENOTEMPTY', 'rmdir'));
      }
      client.exax(cqlg.remove_dir(inf.node),
          make_cass_cb(callback, path, 'rmdir'));
    });
  }


  function mkdir(path, _mode, callback) {
    if (!callback) {
      callback = _mode;
      _mode = 0x777;
    }
    checknul(path, callback);
    pathToNodeid('mkdir', path, -1, function(err, inf) {
      if (err) return callback(err);

      if (inf.node.child && inf.node.child[inf.last]) {
        return callback(new FileError('directory is exists', path, 'EEXIST', 'mkdir'));
      }
      var _c = cqlg.create_dir(inf.node.id, inf.last, _mode, _real_path(path));
      client.exax(_c, make_cass_cb(callback, path, 'mkdir'));
    });
  }


  function readdir(path, callback) {
    checknul(path, callback);
    pathToNodeidQuick('readdir', path, 0, function(err, inf) {
      if (err) return callback(err);
      var list = [];
      for (var n in inf.node.child) {
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
    pathToNodeidQuick('utime', path, 0, function(err, inf) {
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

  function readFile(filename, _options, callback) {}

  function writeFile(filename, data, _options, callback) {}

  function appendFile(filename, data, _options, callback) {}

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
    if (!arguments[i]) {
      throw new Error('null exception');
    }
  }
}


function make_cass_cb(cb, path, syscall, _return_info) {
  return function(err, info) {
    if (err)
      cb(new FileError('fail', path, 'EPERM', syscall, err.code));
    if (_return_info)
      cb(null, info);
    else
      cb();
  }
}
