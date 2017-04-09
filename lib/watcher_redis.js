//
// 不可用
//
function create(_rediscli) {

  return {
    watch  : watch,
    change : change,
    end    : end,
  };

  function end() {
    _rediscli.end();
    _rediscli = null;
  }

  function watch(hdid, filename, _options, _listener) {
    var tfn, channeln,
      cut = notify_perfix.length;
    if (_options.recursive) {
      filename  = _real_path2(filename);
      channeln  = notify_perfix + filename + '*';
      tfn       = 'tpsubscribe';
    } else {
      filename  = _real_path(filename);
      channeln  = notify_perfix + filename;
      tfn       = 'tsubscribe';
    }

    var watcher = new Event();
    var channel = _rediscli[tfn](channeln, function(err, type, _fname) {
      if (err) {
        watcher.emit('error', err);
      } else {
        watcher.emit('change', type, _fname.substr(cut));
      }
      if (!_options.persistent) {
        channel.end();
        watcher.removeAllListeners();
      }
    });

    var index = watchs.push(watcher) - 1;
    watcher.close = function() {
      channel.end();
      watchs[index] = null;
    };

    if (_listener) {
      watcher.on('change', _listener);
    }

    return watcher;
  }


  function change(hdid, filename) {
    var ch = notify_perfix + filename;
    _rediscli.tsend(ch, type, function(err) {
      if (err) logger.error('Send', type, filename, err);
    });
  }

}
