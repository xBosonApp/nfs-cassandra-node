var cqld = require('./cql.js');


module.exports = create_stat;


//
// From Nodejs API docs
//
// `atime` "Access Time" - Time when file data last accessed.
// `mtime` "Modified Time" - Time when file data last modified.
// `ctime` "Change Time" - Time when file status was last changed
//   (inode data modification).
// `birthtime` "Birth Time" - Time of file creation. Set once when the file
//   is created. On filesystems where birthtime is not available,
//   this field may instead hold either the ctime or
//   1970-01-01T00:00Z (ie, unix epoch timestamp 0).
//   Note that this value may be greater than atime or mtime in this case.
//   On Darwin and other FreeBSD variants, also set if the atime is
//   explicitly set to an earlier value than the current birthtime
//   using the utimes(2) system call.
//
function create_stat(inf, hdid) {
  var node = inf.node;
  var pobj = inf.path;
  var ret = {
    dev       : hdid,
    ino       : node.id,
    mode      : node.mode,
    nlink     : node.ref_pth ? node.ref_pth.length : 0,
    uid       : node.uid,
    gid       : node.gid,
    rdev      : hdid,
    size      : node.tsize,
    blksize   : node.blocksz,
    blocks    : parseInt(node.tsize / node.blocksz) + 1,
    atime     : node.atime && new Date(node.atime.toNumber()),
    mtime     : node.mtime && new Date(node.mtime.toNumber()),
    ctime     : node.ctime && new Date(node.ctime.toNumber()),
    birthtime : node.btime && new Date(node.btime.toNumber()),

    isFile            : is_fn(pobj.type == cqld.T_FILE),
    isDirectory       : is_fn(pobj.type == cqld.T_DIR),
    isBlockDevice     : is_fn(true),
    isCharacterDevice : is_fn(false),
    isSymbolicLink    : is_fn(pobj.type == cqld.T_SYM_LINK),
    isFIFO            : is_fn(false),
    isSocket          : is_fn(false),
  };


  return ret;


  function is_fn(attr) {
    return function() {
      return attr;
    };
  }
};
