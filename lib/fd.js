module.exports = create_fd_pool;


function create_fd_pool() {
  var num  = 0;
  var pool = {};
  var ret  = {
    getfd : getfd,
    newfd : newfd,
  };

  next_num();

return ret;


  function getfd(fd_num) {
    if (!pool[fd_num]) {
      var err = new Error('bad file descriptor');
      err.errno = -4083;
      err.code = 'EBADF';
      return;
    }
    return pool[fd_num];
  }


  function newfd(fd_obj) {
    if (!fd_obj) throw new Error('must not null');
    var fd_num = num;
    next_num();
    pool[fd_num] = fd_obj;
    return fd_num;
  }


  function next_num() {
    num += parseInt(Math.random() * 100);
  }
}
