var tool    = require('./tool.js');
var cqld    = require('./cql.js');
var fdp     = require('./fd.js');
var cstat   = require('./stats.js');
var uuid    = require('uuid');
var os      = require('os');
var plib    = require('path').win32;
var util    = require('util');
var logger  = require('logger-lib')('fs-cass');
var hdfs    = require('fs');
var stream  = require('stream');
var Event   = require('events');

var PTNI_FL_NOT_LAST   = 1; // 解析到路径的前一个元素, 保留最后一个
var PTNI_FL_PARSE_LINK = 2; // 如果路径中有连接则重定向
var PTNI_FL_NL_PL      = PTNI_FL_NOT_LAST | PTNI_FL_PARSE_LINK;
var NOTIFY_PERFIX      = 'nfs_cass_notify:';

var constants = {};
for (var n in hdfs.constants) {
  constants[n] = hdfs.constants[n];
}


module.exports = fs;


//
// https://nodejs.org/dist/latest-v0.12.x/docs/api/fs.html
// http://nodeapi.ucdok.com/#/api/fs.html
//
function fs(client, watch_impl, hdid, return_fs_cb) {
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

  var cqlg          = cqld.gen_fs_cql(hdid, client_info);
  var watchs        = [];
  var notify_perfix = NOTIFY_PERFIX + hdid +':';
  var sep           = plib.sep;
  var sep2          = sep + sep;
  var def_mode      = 0666;
  var fd_pool;
  var root_node_id; // 根目录的 nod_id==pth_id
  // var note;


return init();


  function none() {
    var e = new Error('unsupport synchronous function');
    console.log(e);
    throw e;
  }


  function quit(cb) {
    client.exa(cqlg.sub_client_ref(), cb);
    client  = _quit(client);
    cqlg    = _quit(cqlg);
    fd_pool = _quit(fd_pool);

    for (var i=0; i<watchs.length; ++i) {
      if (watchs[i]) {
        watchs[i].close();
      }
    }
    watchs = null;

    function _quit(obj) {
      var ret = {};
      for (var n in obj) {
        if (typeof obj[n] == 'function') {
          ret[n] = function() {
            throw new Error('This FS is closed');;
          };
        }
      }
      return ret;
    }
  }


  function init() {
    client.exa(cqlg.add_client_ref());

    client.exa(cqld.state_drv(hdid).main, function(err, inf) {
      if (err) return return_fs_cb(err);
      if (inf.rows.length <= 0) {
        return return_fs_cb(new FileError('driver not found', hdid));
      }
      root_node_id  = inf.rows[0].root;
      ret.note      = inf.rows[0].note;
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


  function _real_path(p, sp) {
    return sep + parsePathToArr(p).join(sep);
  }


  // redis 模式订阅会转义 '\' 所以必须连接两个
  function _real_path2(p) {
    return sep2 + parsePathToArr(p).join(sep2);
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
  // syscall    -- 函数名
  // p          -- 文件名
  // ptni_flag  -- PTNI_FL_NL_PL 等
  // _checkloop -- 用于软连接循环引用检测, 不要设置
  // cb => (err, node); err 对象可以直接返回给客户
  //
  function pathToNodeid(syscall, p, ptni_flag, cb, _checkloop) {
    var parr  = parsePathToArr(p)
    var i     = -1;
    var len   = parr.length;
    var node, pathobj;
    var _savecb = cb;

    if (!_checkloop) _checkloop = {};

    find_path(root_node_id);

    function find_path(_pth_id) {
      client.exa(cqlg.find_path(_pth_id), function(err, inf) {
        if (err) return cb(
          new FileError(err, p, 'ENOENT', syscall));

        pathobj = inf.rows[0];
        if (!pathobj) return cb(
          new FileError('The path not exists', p, 'ENOENT', syscall));

        find_node(pathobj.nod_id);
      });
    }

    function find_node(_nod_id) {
      client.exa(cqlg.find_node(_nod_id), function(err, inf) {
        if (err) return cb(
          new FileError(err, p, 'ENOENT', syscall));

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
      // console.log('---', node, '@@@', pathobj, i, len, parr, parr[i]);
      if (i+1 < len) {
        if (pathobj.type == cqld.T_SYM_LINK) {
          return parse_link(find_name);
        }
        if (pathobj.type != cqld.T_DIR) {
          return cb(new FileError(
              'path not directory', p, 'ENOTDIR', syscall));
        }
      }

      var name = parr[i];
      if ( pathobj.child && pathobj.child[name]) {
        return find_path(pathobj.child[name]);
      }
      cb(new FileError('Cannot find ' + name +
          ' in', pathobj.complete, 'ENOENT', syscall));
    }

    function parse_link(_next) {
      if (_checkloop[pathobj.sym_link]) {
        if (++_checkloop[pathobj.sym_link] > 5) {
          return cb(new FileError(
            'symbolic links loop', p, 'ELOOP', syscall));
        }
      } else {
        _checkloop[pathobj.sym_link] = 1;
      }

      pathToNodeid(syscall, pathobj.sym_link, ptni_flag, function(err, linf) {
        if (err) {
          err.message = 'link:`' + pathobj.name + '`->' + err.message;
          return cb(err);
        }
        node    = linf.node;
        pathobj = linf.path;
        _next();
      }, _checkloop);
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
        inf.next(function(err, inf) {
          if (err) return cb(err);
          if (inf.path.type != type) {
            return cb(new FileError('Type error', path, 'EDQUOT', syscall));
          }
          cb(null, inf);
        });
      } else {
        var _c;

        if (type == cqld.T_LINK) {
          _c = cqlg.create_link(
              _mode, inf.last, inf.path.id, _real_path(path));

        } else {
          _c = cqlg.create_node(
              inf.path.id, inf.last, _mode, _real_path(path), type, _sym_link);
        }

        client.exax(_c, function(err, r) {
          if (err) return cb(
              new FileError(err.message, path, 'EPERM', syscall, err.code));

          // 创建完成后从 db 拉取完整数据, 之前先关联父目录节点
          if (!inf.path.child) inf.path.child = [];
          inf.path.child[inf.last] = _c.pathid;
          inf.next(cb);
          _change(path, 'create');
          _change(plib.dirname(path), 'addfile', path);
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


  function _change_atime(nid) {
    client.exa(cqlg.atime(nid), function(err) {
      if (err) logger.error('change atime', err);
    });
  }


  function _change_mtime(nid) {
    client.exa(cqlg.mtime(nid), function(err) {
      if (err) logger.error('change mtime', err);
    });
  }


  function create_blocker(filename, node) {
    var bid     = node.id,
        blocksz = node.blocksz,
        tsize   = node.tsize;

    _change_atime(bid);

    var ret = {
      //
      // 总是写入一个块, cb -- Function(err, usesz)
      //
      _write_block: function(page, buffer, usesz, cb) {
        if (buffer.length > blocksz) throw new Error('buffer to biger');

        client.exa(cqlg.write_block(bid, page, buffer, usesz), function(err, inf) {
          if (err) return cb(new FileError(
              err.message, filename, 'EIO', 'write'));
          cb(null, usesz);
        });
      },

      //
      // 如果文件有空洞(虽然写入了高位块, 但是低位块是空的此时 used_size = 0)
      // 总是读取并返回整个块(即使是空块), 并返回有效块长度
      // cb -- Function(err, buffer, used_size)
      //
      _read_block: function(page, cb) {
        client.exa(cqlg.read_block(bid, page), function(err, inf) {
          if (err) return cb(new FileError(err.message, filename, 'EIO', 'read'));
          if (inf.rowLength != 1) {
            return cb(null, 0, new Buffer(blocksz));
          }
          var blk = inf.rows[0];
          cb(null, blk.usesz, blk.block);
        });
      },

      _read_buffer: function(fpos, flen, cb) {
        if (fpos < 0 || flen <= 0) return cb(
          new FileError('bad position (' + fpos + ',' + flen + ')',
            filename, 'EFAULT', 'read'), 0);

        if (fpos + flen > tsize) {
          flen = tsize - fpos;
        }

        var begblk  = parseInt(fpos / blocksz);
        var endblk  = begblk + parseInt(flen / blocksz) + 1;
        var buffer  = new Buffer(flen);
        var page    = begblk;
        var sp      = fpos % blocksz;
        var readlen = 0;

        next_blk();

        function next_blk() {
          if (page < endblk) {
            ret._read_block(page, function(err, usesz, rbuf) {
              if (err) return cb(err);

              if (page == begblk) {
                readlen += rbuf.copy(buffer, 0, sp, rbuf.length);
              } else if (page + 1 == endblk) {
                readlen += rbuf.copy(buffer, readlen, 0, usesz);
              } else {
                readlen += rbuf.copy(buffer, readlen);
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
      _write_buffer_without_chsize: function(fpos, buffer, cb) {
        if (fpos < 0) return cb(
          new FileError('bad position', filename, 'EFAULT', 'write'), 0);

        var begblk    = parseInt(fpos / blocksz);
        var endblk    = begblk + parseInt(buffer.length / blocksz + 1);
        var wbuf      = new Buffer(blocksz);
        var page      = begblk;
        var sp        = fpos % blocksz;
        var usesz     = 0;
        var writelen  = 0;

        if (sp > 0) {
          read_blk(0, first_blk);
        } else {
          first_blk();
        }

        function first_blk() {
          usesz = buffer.copy(wbuf, sp, 0, blocksz - sp);
          writelen += usesz;
          usesz += sp; // 此时必须 +sp 才是使用字节, 否则是拷贝字节
          next_blk();
        }

        function read_blk(beg, next) {
          ret._read_block(page, function(err, rusesz, rbuf) {
            if (err) return cb(err);
            rbuf.copy(wbuf, beg, beg);
            usesz = Math.max(usesz, rusesz);
            next();
          });
        }

        function next_blk() {
          // console.log(', nb', page, '/', endblk, usesz, sp);
          ret._write_block(page, wbuf, usesz, function(err) {
            if (err) return cb(err);
            if (++page < endblk) {
              usesz = buffer.copy(wbuf, 0, writelen, writelen + blocksz);
              writelen += usesz;

              // 最后一个块可能覆盖已有数据, 首先读取再写入
              if (page + 1 == endblk && usesz < blocksz) {
                return read_blk(usesz, next_blk);
              }
              next_blk();

            } else {
              writelen = parseInt(writelen);
              cb(null, writelen, buffer, writelen + fpos);
            }
          });
        }
      }, // END _write_buffer_without_chsize

      _write_buffer: function(fpos, buffer, cb) {
        ret._write_buffer_without_chsize(fpos, buffer,
          function(err, writelen, buffer, nsize) {
            if (err) return cb(err);
            ret._write_size(nsize, function(err) {
              if (err) return cb(err);
              cb(null, writelen, buffer);
            });
          }
        );
      },

      _write_size: function(newsize, cb) {
        if (newsize > tsize) {
          client.exa(cqlg.update_size(bid, newsize), function(err) {
            if (err) return cb(
                new FileError(err.message, filename, 'EIO', 'write'));
            node.tsize = tsize = newsize;
            cb();
          });
        } else {
          _change_mtime(bid);
          cb();
        }
        _change(filename, 'write');
      },

      _truncate: function(size) {
        tsize = size;
      },
    };

    return ret;
  }


  function rename(oldPath, newPath, callback) {
    checknul(oldPath, newPath, callback);
    pathToNodeid('rename', newPath, PTNI_FL_NOT_LAST, function(err, targetdir) {
      if (err) return callback(err);
      if (targetdir.path.child && targetdir.path.child[targetdir.last])
          return callback(new FileError(
            'target is exists', newPath, 'EEXIST', 'rename'));

      pathToNodeid('rename', oldPath, null, function(err, inf) {
        if (err) return callback(err);
        var _c = cqlg.move_path(inf.node.id, inf.path.id, inf.path.parent,
            targetdir.path.id, inf.last, targetdir.last, _real_path(newPath));
        client.exax(_c, make_cass_cb(callback, newPath, 'rename') );
      });
    });
  }


  //
  // ftruncate()会将参数fd指定的文件大小改为参数length指定的大小。
  // 如果原来的文件大小比参数length大，则超过的部分会被删去。
  //
  function ftruncate(fd, len, callback) {
    checknul(fd, len, callback);
    fd_pool.getfd(fd, function(err, inf) {
      client.exax(cqlg.truncate(inf.node.id, len),
          make_cass_cb(warpcb, inf.pathname, 'truncate'));

      function warpcb(err) {
        if (!err) inf.blocker._truncate(len);
        callback(err);
      }
    });
  }


  function truncate(path, len, callback) {
    checknul(path, len, callback);
    pathToNodeid('chown', path, PTNI_FL_PARSE_LINK, function(err, inf) {
      if (err) return callback(err);
      client.exax(cqlg.truncate(inf.node.id, len),
          make_cass_cb(callback, path, 'truncate'));
    });
  }


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


  function fchown(fd, uid, gid, callback) {
    checknul(fd, mode, callback);
    fd_pool.getfd(fd, function(err, inf) {
      if (err) return callback(err);
      var c = cqlg.change_owner(inf.node.id, uid, gid);
      client.exax(c, make_cass_cb(callback, inf.pathname, 'fchown'));
    });
  }


  //
  // 若 path 是一个符号链接时（symbolic link）,
  // 读取的是该符号链接本身，而不是它所 链接到的文件
  //
  function lchown(path, uid, gid, callback) {
    checknul(path, callback, uid, gid);
    pathToNodeid('lchown', path, 0, function(err, inf) {
      if (err) return callback(err);
      var c = cqlg.change_owner(inf.node.id, uid, gid);
      client.exax(c, make_cass_cb(callback, path, 'lchown'));
    });
  }


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


  function fchmod(fd, mode, callback) {
    checknul(fd, mode, callback);
    fd_pool.getfd(fd, function(err, inf) {
      if (err) return callback(err);
      var c = cqlg.change_mode(inf.node.id, mode);
      client.exax(c, make_cass_cb(callback, inf.pathname, 'fchmod'));
    });
  }


  //
  // 若 path 是一个符号链接时（symbolic link）,读取的是该符号链接本身，而不是它所 链接到的文件
  //
  function lchmod(path, mode, callback) {
    checknul(path, mode, callback);
    pathToNodeid('lchmod', path, 0, function(err, inf) {
      if (err) return callback(err);
      var c = cqlg.change_mode(inf.node.id, mode);
      client.exax(c, make_cass_cb(callback, path, 'lchmod'));
    });
  }


  //
  // callback:Function(err, stats) 其中 stats 是一个 fs.Stats 对象
  //
  function stat(path, callback) {
    checknul(path, callback);
    pathToNodeid('stat', path, PTNI_FL_PARSE_LINK, function(err, inf) {
      if (err) return callback(err);
      callback(null, cstat(inf, hdid));
    });
  }


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
      callback(null, cstat(inf, hdid));
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
        return callback(new FileError('not link to dir', p, 'EPERM', 'link'));
      }
      _create_node('link', dstpath, inf.node.id, true, cqld.T_LINK, callback);
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
        if (err) return callback(
          new FileError(err.message, path, 'EPERM', 'unlink'));
        if (inf.path.nod_id) delelte_nod(inf.path.nod_id);
        callback();
        _change(path, 'unlink');
      });
    });
  }


  function rmdir(path, callback) {
    checknul(path, callback);
    pathToNodeid('rmdir', path, 0, function(err, inf) {
      if (err) return callback(err);

      if (inf.path.type != cqld.T_DIR) {
        return callback(
          new FileError('is not directories', path, 'EPERM', 'rmdir'));
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
      _change(path, 'rmdir');
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
    var ftyp = cqld.T_FILE;

    if (flag.not_create) {
      pathToNodeid('open', path, 0, _over);
    } else {
      _create_node('open', path, _mode, flag.throw_if_exist, ftyp, _over);
    }

    function _over(err, inf) {
      if (err) return callback(err, 0);
      if (inf.path.type != ftyp) {
        return callback(
          new FileError('is not file', path, 'EDQUOT', 'open'));
      }
      var fdobj = {
        flag     : flag,
        path     : inf.path,
        node     : inf.node,
        pathname : path,
        blocker  : create_blocker(path, inf.node),
        pos      : flag.append ? inf.node.tsize : 0,
      };
      callback(null, fd_pool.newfd(fdobj));
    }
  }


  //
  // 更改 path 所指向的文件的时间戳。access time, modification time
  //
  function utimes(path, atime, mtime, callback) {
    checknul(path, callback, atime, mtime);
    pathToNodeid('utimes', path, 0, function(err, inf) {
      if (err) return callback(err);

      var c = cqlg.update_time(inf.node.id, atime, mtime);
      client.exax(c, make_cass_cb(callback, path, 'utimes'));
      _change(path, 'utimes')
    });
  }


  function futimes(fd, atime, mtime, callback) {
    checknul(fd, atime, mtime, callback);
    fd_pool.getfd(fd, function(err, inf) {
      if (err) return callback(err);

      var c = cqlg.update_time(inf.node.id, atime, mtime);
      client.exax(c, make_cass_cb(callback, inf.pathname, 'utimes'));
    });
  }


  // 使文件的 modification time 设置为当前系统时间
  function fsync(fd, callback) {
    checknul(fd, callback);
    fd_pool.getfd(fd, function(err, inf) {
      if (err) return callback(err);
      var c = cqlg.fsync(inf.node.id);
      client.exax(c, make_cass_cb(callback, inf.pathname, 'fsync'));
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

      inf.blocker._write_buffer(inf.pos + _position, buffer, function(err,wlen,buf) {
        if (!err) inf.pos += wlen;
        callback(err, wlen, buf);
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
    else if (typeof _options == 'string') {
      _options = { encoding : _options };
    }
    checknul(filename, callback);

    var flag = parse_open_flag(_options.flag || 'r');

    if (!flag.read) {
      return callback(
        new FileError('flag fail', filename, 'EINVAL', 'read'), 0);
    }

    pathToNodeid('read', filename, PTNI_FL_PARSE_LINK, function(err, inf) {
      if (err) return callback(err);
      var node = inf.node;
      var blocker = create_blocker(filename, node)
      blocker._read_buffer(0, node.tsize, function(err, len, buf) {
        if (err) return callback(err);
        if (_options.encoding) {
          buf = buf.toString(_options.encoding);
        }
        callback(null, buf, len);
      });
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
        new FileError('flag fail', filename, 'EINVAL', 'write'), 0, data);
    }

    _create_node('write', filename, _mode, flag.throw_if_exist, cqld.T_FILE,
      function(err, inf) {
        if (err) return callback(err, 0, data);
        var blocker = create_blocker(filename, inf.node);
        blocker._write_buffer(inf.node.tsize, data, callback);
      });
  }


  function watchFile(filename, _options, listener) {
    throw new Error('please use watch()');
  }


  function unwatchFile(filename, _listener) {
    throw new Error('please use watch()');
  }


  // !! 需要完整的安全验证
  function watch(filename, _options, _listener) {
    if (!_listener) {
      if (typeof _options == 'function') {
        _listener = _options;
        _options = null;
      }
    }
    if (!_options) {
      _options = {
        persistent : true,  // true 除非停止否则一直监听
        recursive  : false, // true 递归子目录
        encoding   : 'utf8',
      };
    }

    if (_options.encoding && _options.encoding != 'utf8') {
      throw new Error('encoding unsupport ' + _options.encoding);
    }

    filename = plib.normalize(filename);
    var last = filename[ filename.length-1 ];
    if (last == '/' || last == '\\') {
      filename = filename.substr(0, filename.length-1);
    }

    var _watch = watch_impl.watch(hdid, filename, _options);
    if (_listener) {
      _watch.on('change', _listener);
    }

    return _watch;
  }


  //
  // 发送文件更改消息,
  // filename   -- 发送修改事件的目标路径
  // _real_file -- 实际被修改的文件 (可选)
  function _change(filename, type, _real_file) {
    filename = _real_path(filename);
    watch_impl.change(hdid, filename, type, _real_file);
  }


  function exists(path, callback) {
    pathToNodeidQuick('exists', path, 0, function(err, inf) {
      if (err) return callback(false);
      callback(inf != null);
    });
  }


  function access(path, _mode, callback) {
    pathToNodeidQuick('access', path, 0, function(err, inf) {
      if (err) return callback(err);
      // !! 安全测试不全面
      if (! (inf.node.mode & _mode) ) {
        return callback(new FileError('no access', path, 'EACCES', 'access'));
      }
      callback();
    });
  }


  function createReadStream(path, _options) {
    if (!_options) {
      _options = {
        flags     : 'r',
        encoding  : null,
        fd        : null,
        mode      : def_mode,
        autoClose : true,
        start     : 0,
        end       : -1,
      };
    }

    var closed  = false;
    var fd      = _options.fd;
    var pos     = _options.start || 0;
    var tsize   = _options.end || -1;
    var blksz   = 0;
    var blocker = null;
    var reader  = new stream.Readable(_options);
    var readlen = 0;

    var autoClose = (_options.autoClose !== false);
    if (autoClose) {
      reader.on('end', _close);
      reader.on('error', _close);
    }

    reader._read = function(size) {
      _open(function() {
        blocker._read_buffer(pos, readlen, function(err, rlen, rbuf) {
          if (err) return reader.emit('error', err);
          if (pos >= tsize) {
            reader.push(null);
          } else if (rlen > 0) {
            pos += rlen;
            readlen = blksz;
            reader.push(rbuf);
          } else {
            reader.push(null);
          }
        });
      });
    };

    return reader;

    function _open(_next) {
      if (closed) return reader.emit(
          'error', new Error('reader stream closed'));
      if (fd) return _next();

      open(path, _options.flags, _options.mode, function(err, _fd) {
        if (err) return reader.emit('error', err);

        fd_pool.getfd(_fd, function(err, fdobj) {
          if (err) return reader.emit('error', err);
          fd = _fd;
          blocker = fdobj.blocker;
          if (pos < 0) pos = 0;
          if (tsize <= 0 || tsize < pos || tsize > fdobj.node.tsize) {
            tsize =  fdobj.node.tsize;
          }
          blksz =  fdobj.node.blocksz;
          readlen = blksz - parseInt(pos % blksz);
          _next();
          reader.emit('open', fd);
        });
      });
    }

    function _close(errclose) {
      if (!fd) return;
      close(fd, function(err) {
        // if (err) reader.emit('error', err);
        if (!errclose) reader.emit('close');
      });
      fd      = null;
      blocker = null;
      closed  = true;
    }
  }


  function createWriteStream(path, _options) {
    if (!_options) {
      _options = {
        flags     : 'w',
        encoding  : null,
        mode      : def_mode,
        start     : 0,
      };
    }

    var fd, tsize, blksz, blocker, wbuf;
    var pos     = _options.start || 0;
    var writer  = new stream.Writable(_options);
    var twsize  = 0;

    var autoClose = (_options.autoClose !== false);
    if (autoClose) {
      writer.on('error', _close);
      writer.on('finish', _close);
    }

    writer._write = function(chunk, encoding, callback) {
      var buf;
      var sp = 0;

      _open(function() {
        // 尽可能使写入的块与底层块对齐, 避免非对齐时的写入前读取
        wbuf = chunk.slice(sp, blksz - parseInt(pos % blksz));
        next_write();
      });

      function next_write() {
        blocker._write_buffer_without_chsize(pos + sp, wbuf, function(err, wl, _, wsize) {
          if (err) return callback(err);
// if (path=='2.txt') console.log('->>>', path,  sp, twsize, wsize, wbuf.length, wbuf, chunk, '\n');
          sp += wbuf.length;
          twsize = wsize;
          if (sp < chunk.length) {
            wbuf = chunk.slice(sp, sp + blksz);
            next_write();
          } else {
            pos += sp;
            callback();
          }
        });
      }
    };

    return writer;


    function _open(_next) {
      if (fd) return _next();

      open(path, _options.flags, _options.mode, function(err, _fd) {
        if (err) return writer.emit('error', err);

        fd_pool.getfd(_fd, function(err, fdobj) {
          if (err) return writer.emit('error', err);
          tsize   = fdobj.node.tsize;
          blksz   = fdobj.node.blocksz;
          blocker = fdobj.blocker;
          fd      = _fd;
          writer.emit('open', fd);
        });

        ftruncate(fd, /*_options.start*/ pos, _next);
        // _next();
      });
    }

    function _close(enderr) {
      if (!fd) return;
      var tmpfd = fd;

      blocker._write_size(twsize, function(err) {
        if (err) writer.emit('error', err);
        close(tmpfd, function(err) {
          // if (err) writer.emit('error', err);
          if (!enderr) writer.emit('close');
        });
      });

      fd = blocker = null;
    }
  }
}


//  errno: -4058, code: 'ENOENT', syscall: 'mkdir', path: 'x:\\a'
function FileError(msg, path, code, syscall, errno) {
  if (path) path = ': `' + path + '`';
  else path = '';

  if (util.isError(msg)) {
    this.errno    = msg.errno || msg.code || -1;
    this.code     = msg.code  || code || 'UNKNOW';
    this.stack    = msg.stack;
    this.message  = msg.message + path;
  } else {
    this.errno    = errno || '-1';
    this.code     = code  || 'UNKNOW';
    this.message  = msg + path;
    this.stack    = new Error().stack;
  }

  this.path       = path;
  this.syscall    = syscall || 'UNKNOW';
}

util.inherits(FileError, Error);


function checknul() {
  for (var i=0, e=arguments.length; i<e; ++i) {
    if ((!arguments[i]) && isNaN(arguments[i]) ) {
      throw new Error('The arguments ' + i + ' is null');
    }
  }
}


function make_cass_cb(cb, path, syscall, _return_info) {
  return function(err, info) {
    //err && console.log('!!! make_cass_cb', err);
    if (err)
      cb(new FileError(err, path, 'EPERM', syscall));
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
    not_create          : false,
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

  if (ret.read && ret.write == false && ret.append == false) {
    ret.not_create = true;
  }

  return ret;
}
