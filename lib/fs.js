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
    
  function ftruncate(fd, len, callback) {}
    
  function truncate(path, len, callback) {}
    
  function chown(path, uid, gid, callback) {}
    
  function fchown(fd, uid, gid, callback) {}
    
  function lchown(path, uid, gid, callback) {}
    
  function chmod(path, mode, callback) {}
    
  function fchmod(fd, mode, callback) {}
    
  function lchmod(path, mode, callback) {}
    
  function stat(path, callback) {}
    
  function lstat(path, callback) {}
    
  function fstat(fd, callback) {}
    
  function link(srcpath, dstpath, callback) {}
    
  function symlink(srcpath, dstpath, _type, callback) {}
    
  function readlink(path, callback) {}
    
  function realpath(path, _cache, callback) {}
    
  function unlink(path, callback) {}
    
  function rmdir(path, callback) {}
    
  function mkdir(path, _mode, callback) {}
    
  function readdir(path, callback) {}
    
  function close(fd, callback) {}
    
  function open(path, flags, _mode, callback) {}
    
  function utimes(path, atime, mtime, callback) {}
    
  function futimes(fd, atime, mtime, callback) {}  
    
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