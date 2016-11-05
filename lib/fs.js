var tool  = require('./tool.js');
var cqld  = require('./cql.js');
var fdp   = require('./fd.js');
var cstat = require('./stats.js');
var uuid  = require('uuid');
var os    = require('os');
var plib  = require('path').win32;
var util  = require('util');
var logger= require('logger-lib')('fs-cass');
var hdfs  = require('fs');

var PTNI_FL_NOT_LAST   = 1;
var PTNI_FL_PARSE_LINK = 2;
var PTNI_FL_NL_PL      = PTNI_FL_NOT_LAST | PTNI_FL_PARSE_LINK;

var constants = {};
for (var n in hdfs.constants) {
  constants[n] = hdfs.constants[n];
}


module.exports = fs;


//
// https://nodejs.org/dist/latest-v0.12.x/docs/api/fs.html
// http://nodeapi.ucdok.com/#/api/fs.html
// !!! 循环链接检查
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
    constants         : constants,

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
  // 但不支持权限, 不支持链接
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
  // syscall  -- 函数名
  // cb => (err, node); err 对象可以直接返回给客户
  //
  function pathToNodeid(syscall, p, ptni_flag, cb) {
    var parr  = parsePathToArr(p)
    var i     = -1;
    var len   = parr.length;
    var node, pathobj;
    var _savecb = cb;

    find_path(root_node_id);

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

    function find_node(_nod_id) {
      client.exa(cqlg.find_node(_nod_id), function(err, inf) {
        if (err) return cb(
          new FileError(err.message, p, 'ENOENT', syscall, err.code));

        node = inf.rows[0];
        if (!node) return cb(
          new FileError('The node not exists', p, 'ENOENT', syscall));

        ++i;
        if ((PTNI_FL_NOT_LAST & ptni_flag) && i == len - 1) {
          call_over();

        } else if (i < len) {
          find_name();

        } else if ((PTNI_FL_PARSE_LINK & ptni_flag)
                && pathobj.type == cqld.T_SYM_LINK) {
          parse_link(call_over);

        } else {
          call_over();
        }
      });
    }

    function find_name() {
      // console.log('---', node, '@@@', pathobj);
      // console.log(i, len, parr, parr[i]);
      if (i+1 < len) {
        if (pathobj.type == cqld.T_SYM_LINK) {
          return parse_link(find_name);
        }
        if (pathobj.type != cqld.T_DIR) {
          return cb(new FileError('path not directory', p, 'ENOTDIR', syscall));
        }
      }

      var name = parr[i];
      if ( pathobj.child && pathobj.child[name]) {
        return find_path(pathobj.child[name]);
      }
      cb(new FileError('Cannot find ' + name + ' in', p, 'ENOENT', syscall));
    }

    function parse_link(_next) {
      pathToNodeid(syscall, pathobj.sym_link, null, function(err, linf) {
        if (err) {
          err.message = 'link:`' + pathobj.name + '`->' + err.message;
          return cb(err);
        }
        node    = linf.node;
        pathobj = linf.path;
        _next();
      });
    }

    function call_over() {
      cb(null, {
        node : node,
        path : pathobj,
        last : parr[parr.length-1],
        next : _next,
        path_arr : parr,
      });
      function _next(_chang_cb) {
        if (_chang_cb) cb = _chang_cb;
        else cb = _savecb;
        find_name();
      }
    }
  }


  //
  // 获取或创建 type 类型节点, throw_if_exist=true 则存在时抛出异常
  // 创建与获取返回值不同
  //
  function _create_node(syscall, path, _mode, throw_if_exist, type, cb, _sym_link) {
    pathToNodeid(syscall, path, PTNI_FL_NL_PL, function(err, inf) {
      if (err) return cb(err);

      if (inf.path.child && inf.path.child[inf.last]) {
        if (throw_if_exist) {
          return cb(new FileError('is exists', path, 'EEXIST', syscall));
        }
        inf.next(cb);

      } else {
        var _c;

        if (type == cqld.T_LINK) {
          _c = cqlg.create_link(
              inf.node.id, inf.last, inf.path.id, _real_path(path));

        } else {
          _c = cqlg.create_node(
              inf.path.id, inf.last, _mode, _real_path(path), type, _sym_link);
        }

        client.exax(_c, function(err, r) {
          if (err) return cb(
              new FileError('fail', path, 'EPERM', syscall, err.code));
          cb(null, {
            node : _c.block,
          });
        });
      }
    });
  }


  function delelte_nod(id) {
    var c = cqlg.delete_nod(id);
    client.exa(c.check, function(err, inf) {
      if (err) {
        logger.error(err);
      } else if (inf.rowLength == 1
            && (inf.rows[0].ref_pth == null || inf.rows[0].ref_pth.length <= 0) ) {
        client.exax(c.delete, function(err) {
          if (err) logger.error(err);
        });
      }
    });
  }


  function create_blocker(filename, node) {
    var bid     = node.id,
        blocksz = node.blocksz,
        tsize   = node.tsize;

    var ret = {
      _write_block: function(page, buffer, cb) {
        if (buffer.length > blocksz) throw new Error('buffer to biger');

        client.exa(cqlg.write_block(bid, page, buffer), function(err, inf) {
          if (err) return cb(new FileError(err.message, filename, 'EIO', 'write'));
          cb(null, blocksz);
        });
      },

      //
      // 如果文件有空洞(虽然写入了高位块, 但是低位块是空的此时 is_none_block = true)
      // cb -- Function(err, buffer, is_none_block)
      //
      _read_block: function(page, cb) {
        client.exa(cqlg.read_block(bid, page), function(err, inf) {
          if (err) return cb(new FileError(err.message, filename, 'EIO', 'read'));
          if (inf.rowLength != 1) {
            return cb(null, new Buffer(blocksz), true);
          }
          cb(null, inf.rows[0].block);
        });
      },

      _read_buffer: function(fpos, flen, cb) {
        if (fpos < 0 || flen <= 0) return cb(
          new FileError('bad position', filename, 'EFAULT', 'read'), 0);

        var begblk  = parseInt(fpos / blocksz);
        var endblk  = parseInt((fpos + flen) / blocksz) + 1;
        var buffer  = new Buffer(flen);
        var page    = begblk;
        var sp      = fpos % blocksz;
        var readlen = 0;

        next_blk();

        function next_blk() {
          if (page < endblk) {
            ret._read_block(page, function(err, rbuf, is_none_block) {
              if (err) return cb(err);

              if (page == begblk) {
                rbuf.copy(buffer, 0, sp, rbuf.length);
                readlen += rbuf.length - sp;
              } else {
                rbuf.copy(buffer, readlen);
                readlen += rbuf.length;
              }

              ++page;
              next_blk();
            });
          } else {
            over();
          }
        }

        function over() {
          readlen = Math.min(readlen, flen);
          buffer  = buffer.slice(0, readlen);
          cb(null, readlen, buffer);
        }
      }, // END _read_buffer

      // 当 fpos 与 block 不对齐时要考虑先读取 block 并填充不对齐的数据
      _write_buffer: function(fpos, buffer, cb) {
        if (fpos < 0) return cb(
          new FileError('bad position', filename, 'EFAULT', 'write'), 0);

        var begblk    = parseInt(fpos / blocksz);
        var endblk    = parseInt(buffer.length / blocksz) + 1;
        var wbuf      = new Buffer(blocksz);
        var page      = begblk;
        var sp        = fpos % blocksz;
        var writelen  = 0;

        if (sp > 0) {
          read_blk(0, first_blk);
        } else {
          first_blk();
        }

        function first_blk() {
          writelen += buffer.copy(wbuf, sp, 0, blocksz - sp);
          next_blk();
        }

        function read_blk(beg, next) {
          ret._read_block(page, function(err, rbuf, is_none_block) {
            if (err) return cb(err);
            rbuf.copy(wbuf, beg, beg);
            next();
          });
        }

        function next_blk() {
          ret._write_block(page, wbuf, function(err) {
            if (err) return cb(err);
            if (++page < endblk) {
              writelen += buffer.copy(wbuf, 0, writelen, writelen + blocksz);

              if (sp > 0 && page + 1 == endblk) {
                read_blk(sp, next_blk);
              } else {
                next_blk();
              }
            } else {
              write_size();
            }
          });
        }

        function write_size() {
          writelen = parseInt(writelen);
          if (writelen + fpos != tsize) {
            client.exa(cqlg.update_size(bid, writelen + fpos), function(err) {
              if (err) return cb(
                new FileError(err.message, filename, 'EIO', 'write'),
                writelen, buffer);

              cb(null, writelen, buffer);
            });
          } else {
            cb(null, writelen, buffer);
          }
        }
      }, // END _write_buffer
    };

    return ret;
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
    pathToNodeid('chown', path, PTNI_FL_PARSE_LINK, function(err, inf) {
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
    pathToNodeid('chmod', path, PTNI_FL_PARSE_LINK, function(err, inf) {
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


  function fstat(fd, callback) {
    checknul(fd, callback);
    fd_pool.getfd(fd, function(err, inf) {
      if (err) return callback(err);
    });
  }


  //
  // creates a new link (hard link) to an existing file
  //
  function link(srcpath, dstpath, callback) {
    checknul(srcpath, dstpath, callback);
    pathToNodeid('link', srcpath, 0, function(err, inf) {
      if (err) return callback(err);
      if (inf.path.type == cqld.T_DIR) {
        return callback(
          new FileError('not link to dir', p, 'EPERM', 'link'));
      }
      _create_node('link', dstpath, null, true, cqld.T_LINK, callback);
    });
  }


  function symlink(srcpath, dstpath, _type, callback) {
    if (!callback) {
      callback = _type;
      _type;
    }
    checknul(srcpath, dstpath, callback);
    pathToNodeid('symlink', srcpath, 0, function(err, inf) {
      if (err) return callback(err);
      _create_node('symlink', dstpath,
          def_mode, true, cqld.T_SYM_LINK, callback, _real_path(srcpath));
    });
  }


  //
  // 返回符号链接指向的文件名, callback:(err, linkString)
  //
  function readlink(path, callback) {
    pathToNodeid('readlink', path, 0, function(err, inf) {
      if (err) return callback(err);
      callback(null, inf.path.sym_link);
    });
  }


  //
  // process.cwd 可能定位到相对路径, 返回完整路径
  //
  function realpath(path, _cache, callback) {
    checknul(path, callback);
    if (!callback) {
      callback = _cache;
    }
    path = _real_path(path);
    if (_cache && _cache[path]) {
      callback(null, _cache[path]);
    } else {
      callback(null, path);
    }
  }


  //
  // deletes a name from the file system
  //
  function unlink(path, callback) {
    checknul(path, callback);
    pathToNodeid('unlink', path, 0, function(err, inf) {
      if (err) return callback(err);
      if (inf.path.type == cqld.T_DIR) return callback(
        new FileError('is directories', path, 'EPERM', 'unlink'));

      client.exax(cqlg.unlink(inf.path), function(err) {
        if (err) return callback(new FileError(err.message, path, 'EPERM', 'unlink'));
        if (inf.path.nod_id) delelte_nod(inf.path.nod_id);
        callback();
      });
    });
  }


  function rmdir(path, callback) {
    checknul(path, callback);
    pathToNodeid('rmdir', path, 0, function(err, inf) {
      if (err) return callback(err);

      if (inf.path.type != cqld.T_DIR) {
        return callback(new FileError('is not directories', path, 'EPERM', 'unlink'));
      }
      if (inf.path.child) {
        return callback(new FileError(
          'Directory not empty', path, 'ENOTEMPTY', 'rmdir'));
      }
      if (inf.path.name == plib.sep) {
        return callback(new FileError(
          'cannot remove root', path, 'EBUSY', 'rmdir'));
      }
      client.exax(cqlg.unlink(inf.path),
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
    pathToNodeid('readdir', path, PTNI_FL_PARSE_LINK, function(err, inf) {
      if (err) return callback(err);
      var list = [];
      for (var n in inf.path.child) {
        list.push(n);
      }
      callback(null, list);
    });
  }


  function close(fd, callback) {
    fd_pool.remove(fd, callback);
  }


  function open(path, flags, _mode, callback) {
    if (!callback) {
      callback = _mode;
      _mode = def_mode;
    }
    checknul(path, callback, flags);
    var flag = parse_open_flag(flags);

    _create_node('open', path, _mode, flag.throw_if_exist, cqld.T_FILE,
      function(err, inf) {
        if (err) return callback(err, 0, data);
        var fdobj = {
          flag     : flag,
          path     : inf.path,
          node     : inf.node,
          pathname : path,
          blocker  : create_blocker(path, inf.node),
          pos      : flag.append ? inf.node.tsize : 0,
        };
        callback(null, fd_pool.newfd(fdobj));
      });
  }


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


  function futimes(fd, atime, mtime, callback) {
    checknul(fd, atime, mtime, callback);
    fd_pool.getfd(fd, function(err, inf) {
      if (err) return callback(err);

      var c = cqlg.update_time(inf.node.id, atime, mtime);
      client.exax(c, make_cass_cb(callback, inf.pathname, 'utime'));
    });
  }


  // 使文件的 modification time 设置为当前系统时间
  function fsync(fd, callback) {
    checknul(fd, callback);
    fd_pool.getfd(fd, function(err, inf) {
      if (err) return callback(err);
      var c = cqlg.fsync(inf.node.id);
      client.exax(c, make_cass_cb(callback, inf.pathname, 'syncfs'));
    });
  }


  // string mode (fd, data[, position[, encoding]], callback)
  function write(fd, buffer, offset, length, _position, callback) {
    var encoding;
    if (isNaN(length)) { // string mode
      callback    = _position;
      _position   = offset || 0;
      encoding    = length;

      if (!encoding) {
        callback  = _position;
        _position = 0;
        encoding  = 'utf8';
      }
      else if (!callback) {
        callback  = encoding;
        encoding  = 'utf8';
      }

      checknul(fd, buffer, callback);
      if (!util.isBuffer(buffer)) {
        buffer = new Buffer(buffer, encoding);
      }
    } else {
      if (!callback) {
        callback = _position;
        _position = 0;
      }
      checknul(fd, buffer, offset, length, callback);
      buffer = buffer.slice(offset, offset + length);
    }

    fd_pool.open_use_flat(fd, 'write', function(err, inf) {
      if (err) return callback(err);

      inf.blocker._write_buffer(inf.pos + _position, buffer, function(a,b,c) {
        if (!a) inf.pos += b;
        callback(a,b,c);
      });
    });
  }


  //
  // 读取超过文件长度的内容是未定义的
  //
  function read(fd, buffer, offset, length, position, callback) {
    checknul(fd, buffer, offset, length, callback);
    fd_pool.open_use_flat(fd, 'read', function(err, inf) {
      if (err) return callback(err);
      if (isNaN(position)) position = inf.pos;
      var rlen = length - offset;

      inf.blocker._read_buffer(position, rlen, function(err, rlen0, buf) {
        if (err) return callback(err);
        var copylen = buf.copy(buffer, offset, 0, rlen0);
        callback(null, copylen, buffer);
      });
    });
  }


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

    pathToNodeid('read', filename, PTNI_FL_PARSE_LINK, function(err, inf) {
      if (err) return callback(err);
      var node = inf.node;
      var blocker = create_blocker(filename, node)
      blocker._read_buffer(0, node.tsize, callback);
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
        var blocker = create_blocker(filename, inf.node);
        blocker._write_buffer(0, data, callback);
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
        new FileError('flag fail', filename, 'EINVAL', 'append'), 0, data);
    }

    _create_node('write', filename, _mode, flag.throw_if_exist, cqld.T_FILE,
      function(err, inf) {
        if (err) return callback(err, 0, data);
        var blocker = create_blocker(filename, inf.node);
        blocker._write_buffer(inf.node.tsize, data, callback);
      });
  }


  function watchFile(filename, _options, listener) {}

  function unwatchFile(filename, _listener) {}

  function watch(filename, _options, _listener) {}


  function exists(path, callback) {
    pathToNodeidQuick('exists', path, 0, function(err, inf) {
      if (err) return callback(false);
      callback(true);
    });
  }


  function access(path, _mode, callback) {
    pathToNodeidQuick('access', path, 0, function(err, inf) {
      if (err) return callback(err);
      if (! (inf.node.mode & _mode) ) {
        return callback(new FileError('no access', path, 'EACCES', 'access'));
      }
      callback();
    });
  }


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
    // err && console.log('!!! make_cass_cb', err);
    if (err)
      cb(new FileError(err.message, path, 'EPERM', syscall, err.code));
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
        ret.write  = true;
        ret.append = true;
        break;
    }
  }

  return ret;
}
