module.exports = {
  create : fs
};

//
// https://nodejs.org/dist/latest-v0.12.x/docs/api/fs.html
// http://nodeapi.ucdok.com/#/api/fs.html
//
function fs() {
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
  };

  function none() {
    throw new Error('unsupport synchronous function');
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
    
  function mkdir(path, _mode, callback) {}
    
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