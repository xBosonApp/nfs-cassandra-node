module.exports = {
  open_driver : open_driver,
};


function open_driver(_client) {
  var ret = {
    create    : null,
    delete    : null,
    list      : null,
    open_fs   : null,
    close_fs  : null,
  };

  if (!_client) {
    throw new Error('client is null');
  }
  
  return ret;
}