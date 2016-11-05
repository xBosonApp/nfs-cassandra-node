module.exports = create_fd_pool;


function create_fd_pool() {
  var num  = 0;
  var pool = {};
  var ret  = {
    getfd         : getfd,
    newfd         : newfd,
    remove        : remove,
    open_use_flat : open_use_flat,
  };

  next_num();

return ret;


  function getfd(fd_num, cb) {
    if (!pool[fd_num]) {
      var err = new Error('bad file descriptor');
      err.errno = -1;
      err.code = 'EBADF';
      return cb(err);
    }
    cb(null, pool[fd_num]);
  }


  function newfd(fd_obj) {
    if (!fd_obj) throw new Error('must not null');
    var fd_num = num;
    next_num();
    pool[fd_num] = fd_obj;
    return fd_num;
  }


  function open_use_flat(fdn, flag, cb) {
    getfd(fdn, function(err, inf) {
      if (err) return cb(err);
      if (!inf.flag[flag]) {
        var err = new Error('bad flag');
        err.errno = -3;
        err.code = 'EINVAL';
        return cb(err);
      }
      cb(null, inf);
    });
  }


  function remove(fdnum, cb) {
    if (!pool[fdnum]) {
      var err = new Error('bad file descriptor');
      err.errno = -2;
      err.code = 'EBADF';
      return cb(err);
    }
    delete pool[fdnum]
    cb(null, fdnum);
  }


  function next_num() {
    num += parseInt(Math.random() * 100);
  }
}
