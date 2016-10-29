var tool = require('./tool.js');
var cqld = require('./cql.js');
var fdp  = require('./fd.js');
var uuid = require('uuid');
var os   = require('os');
var plib = require('path').win32;


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

    client.exa(cqld.state_drv(hdid), function(err, inf) {
      if (err) return return_fs_cb(err);
      root_node_id = inf.rows[0].root;
      note = inf.rows[0].note;
      fd_pool = fdp();
      cb(null, ret);
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


  //
  // 通过文件路径获取节点
  // offset -- 遍历到 p.length - offset
  // cb => (err, node)
  //
  function pathToNodeid(p, offset, cb) {
    var parr = parsePathToArr(p)
    var i = -1;
    var node;

    find_node(root_node_id);

    function find_node(_id, _type) {
      // !! 遍历节点时没有判断类型
      client.exa(cqlg.find_node(_id), function(err, inf) {
        if (err) return cb(err);
        node = inf.rows[0];
        find_name(node.child);
      });
    }

    function find_name(child) {
      if (++i < parr.length - offset) {
        var name = parr[i];
        var _id = child[name];
        if (_id) {
          find_node(_id);
        } else {
          cb(new Error('cannot find'));
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

  //  chown将指定文件的拥有者改为指定的用户或组
  function chown(path, uid, gid, callback) {}

  function fchown(fd, uid, gid, callback) {}

  // 若 path 是一个符号链接时（symbolic link）,读取的是该符号链接本身，而不是它所 链接到的文件
  function lchown(path, uid, gid, callback) {}

  // 修改文件权限
  function chmod(path, mode, callback) {}

  function fchmod(fd, mode, callback) {}

  // 若 path 是一个符号链接时（symbolic link）,读取的是该符号链接本身，而不是它所 链接到的文件
  function lchmod(path, mode, callback) {}

  // callback:Function(err, stats) 其中 stats 是一个 fs.Stats 对象。 详情请参考 fs.Stats
  function stat(path, callback) {}

  // 若 path 是一个符号链接时（symbolic link）,读取的是该符号链接本身，而不是它所 链接到的文件
  function lstat(path, callback) {}

  function fstat(fd, callback) {}

  // creates a new link (hard link) to an existing file
  function link(srcpath, dstpath, callback) {}

  // 与link 相同
  function symlink(srcpath, dstpath, _type, callback) {}

  // 返回符号链接指向的文件名, callback:(err, linkString)
  function readlink(path, callback) {}

  // process.cwd 可能定位到相对路径, 返回完整路径
  function realpath(path, _cache, callback) {}

  // deletes a name from the file system
  function unlink(path, callback) {}

  function rmdir(path, callback) {}

  function mkdir(path, _mode, callback) {
    if (!_mode) {
      callback = _mode;
      _mode = 0x7 + (0x7 << 4) + (0x7 << 8);
    }
    pathToNodeid(path, -1, function(err, inf) {
      if (err) return callback(err);
      if (inf.node.child[inf.last]) {
        return callback(new Error('dir '+ path + ' is exists'));
      }
      client.exax(cqlg.create_dir(inf.node.id, inf.last, _mode),
        function(err, inf) {
          callback(err);
        });
    });
  }

  function readdir(path, callback) {}

  function close(fd, callback) {}

  function open(path, flags, _mode, callback) {}

  // 更改 path 所指向的文件的时间戳。access time, modification time
  function utimes(path, atime, mtime, callback) {}

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
