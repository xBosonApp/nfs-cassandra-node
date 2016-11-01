var cqld = require('./cql.js');


module.exports = create_stat;


//
// mtime "Modified Time" - 文件上次被修改的时间。
//  会被 mknod(2), utimes(2), and write(2) 等系统调用改变。
// ctime "Change Time" - 文件状态上次改变的时间。 (inode data modification).
//  会被 chmod(2), chown(2), link(2), mknod(2), rename(2), unlink(2), utimes(2),
//  read(2), and write(2) 等系统调用改变。
//
function create_stat(node, hdid) {
  var ret = {
    dev       : hdid,
    ino       : node.id,
    mode      : node.mode,
    nlink     : 0,  /* number of hard links */
    uid       : node.uid,
    gid       : node.gid,
    rdev      : hdid,
    size      : 0,
    blksize   : 0,
    blocks    : 0,
    atime     : node.atime && new Date(node.atime.toNumber()),
    mtime     : node.mtime && new Date(node.mtime.toNumber()),
    ctime     : node.ctime && new Date(node.ctime.toNumber()),
    birthtime : node.btime && new Date(node.btime.toNumber()),

    isFile            : is_fn(node.type == cqld.T_FILE),
    isDirectory       : is_fn(node.type == cqld.T_DIR),
    isBlockDevice     : is_fn(true),
    isCharacterDevice : is_fn(false),
    isSymbolicLink    : is_fn(node.type == cqld.T_SYM_LINK),
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
