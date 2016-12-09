var plib   = require('path');
var stream = require('stream');

module.exports = function createImpl(
  cmd, _setPrefix, log, driverlib, stdin, stdout) {

// 不要直接引用, 使用 ckfs() 和 ckdr()
var fs, driver;
var hdids, hdinf;
var dvid, dvidx, hdidx, hdid;
var root = '\\';
var helper = {};

cmd.quit = cmd.exit = function() {
  process.exit(0);
};

cmd.help = function(argv, next) {
  log();
  var sub = argv[1];
  if (!sub) {
    log('help       -- print help infomation');
    log('exit, quit -- quit the console');
    log('open       -- open remote driver');
    log('list       -- list all hard disk and index');
    log('update     -- update hard dist list');
    log('use        -- open hard disk with hdid');
    log('mkhd       -- crate hard disk');
    log('rmhd       -- remove hard disk');
    log('sw         -- switch driver/fs');
    log();
    log('ls         -- list dir files');
    log('cd         -- change work directory');
    log('cat        -- concat input and output');
    log('stat       -- show file state');
    log('rm         -- remove file');
    log('mv         -- move file');
    log('mkdir      -- make dir');
    log('cp         -- copy file content');
  } else if (helper[sub]) {
    log('Usage:', sub, helper[sub]);
  } else {
    log('Command:', sub, 'no help');
  }
  next();
};

helper.test = '[do nothing]';
cmd.test = function test(argv, next) {
  log('ARGV:', argv);
  next();
};

helper.sw = '[hd-index] [driver-index]';
cmd.sw = function(av, next) {
  throw new Error('not implement');
};

helper.open = '[--]';
cmd.open = function(argv, next) {
  log('use default config to connect');

  driverlib.open_driver(function(err, _driver) {
    if (err) return log(err.message), next();
    driver = _driver;
    dvid   = 0;
    dvidx  = 0;

    cmd.update(argv, function() {
      log('nfs driver opend');
      update_prompt();
      next();
    });
  });
};

helper.mkhd = '[note]';
cmd.mkhd = function(av, next) {
  var d = ckdr();
  var note = av[1] || 'console-hard-disk-' + hdids.length;
  d.create(note, function(err, inf) {
    if (err) {
      log(err.message);
      next();
    } else {
      log('HDID:', inf.hd_id, 'created');
      cmd.update(0, next);
    }
  });
};

helper.rmhd = '[hdid]'
cmd.rmhd = function(av, next) {
  var d = ckdr();
  var id = hdids[ av[1] ] || av[1];
  d.delete(id, function(err) {
    if (err) {
      log(err.message);
      next();
    } else {
      log('HDID:', id, 'removed');
      cmd.update(0, next);
    }
  });
};

helper.update = '(no arguments)';
cmd.update = function(argv, next) {
  ckdr().list(function(err, ids) {
    if (err) {
      log(err.message);
      next();
    } else {
      hdids = ids;
      hdinf = [];
      var size = hdids.length-1;
      hdids.forEach(function(id, index) {
        ckdr().state(id, function(err, s) {
          if (err) {
            log(err.message);
            hdinf[index] = err.message;
          } else {
            hdinf[index] = s;
          }
          if (--size < 0) {
            log('update hd list');
            next();
          }
        });
      });
    }
  });
};

helper.list = '(no arguments)';
cmd.list = function(a, next) {
  ckdr();
  log('\n[index] [info]');
  hdinf.forEach(function(s, i) {
    log('', i, '\t', s);
  });
  next();
};

helper.use = '{[hd-index] | [hd-id]}'
cmd.use = function(a, next) {
  if (!hdids) ckdr();
  var i = a[1];
  var id;

  if (i === undefined) {
    id = hdids[0];
    if (id) {
      i  = 0;
      log('open default 0 hard disk');
    }
  } else if (!isNaN(i)) {
    id = hdids[i];
    if (!id) {
      log(id, 'is not valid hdid');
      return next();
    }
    log('open hard disk', id, 'with index:', hi);
  } else if (i.indexOf('-') > 0) {
    id = i;
    i  = null;
    log('open hard disk with id:', id);
    for (var j=0; j<hdids.length; ++j) {
      if (hdids[j] == id) {
        i = j;
        break;
      }
    }
  }

  if (!id) {
    log('cannot find any hard disk');
    return next();
  }

  ckdr().open_fs(id, function(err, xfs) {
    if (err) log(err.message);
    else {
      fs    = xfs;
      hdid  = id;
      hdidx = i;
      log('fs opend');
      update_prompt();
    }
    next();
  });
};

helper.ls = '(no arguments)'
cmd.ls = function(av, next) {
  ckfs().readdir(root, function(err, files) {
    var out = [];
    var i =-1;
    nfile();

    function nfile() {
      if (++i < files.length) {
        ckfs().stat(files[i], function(e, s) {
          if (e) {
            log(e.message);
            return next();
          }
          if (s.isDirectory()) {
            out.push("\u001B[94m");
          } else if (s.isSymbolicLink()) {
            out.push("\u001B[92m");
          } else {
            out.push("\u001B[39m");
          }
          out.push(files[i]);
          out.push('\t');
          nfile();
        });
      } else {
        out.push("\u001B[39m");
        log(out.join(''));
        next();
      }
    }
  });
};

helper.cd = 'dirname';
cmd.cd = function cd(av, next) {
  needav(av[1]);
  if (av[1] == '/' || av[1] == '\\') {
    root = '\\';
    update_prompt();
    return next();
  }
  var cdir = fix(av[1]);

  ckfs().stat(cdir, function(err, s) {
    if (err) log(err.message);
    else if (s.isDirectory()) {
      root = cdir;
      update_prompt();
    } else {
      log('fail:', cdir, 'is not dir');
    }
    next();
  });
};

helper.cat = '[input-file]';
cmd.cat = function(av, next) {
  var r = av[1] ? cmd._openReadStream(av[1]) : stdin();
  var w = stdout();
  if (r.isTTY) {
    _bind();
  } else {
    r.pipe(w);
  }
  r.on('end',   _close);
  r.on('error', _close);
  w.on('error', _close);

  function _close(e) {
    if (e) return console.error(e);
    r.removeAllListeners('data');
    next();
  }
  function _bind() {
    r.on('data', function(chunk) {
      var ch = chunk[0];
      if (ch == 0x0d || ch == 0x0a) {
        w.end(_close);
      } else {
        w.write(chunk);
      }
    });
  }
};

helper.stat = 'filename';
cmd.stat = function stat(av, next) {
  needav(av[1]);
  var cpath = fix(av[1]);
  ckfs().stat(cpath, function(e, s) {
    if (e) log(e.message);
    else {
      log(cpath, '\t', s.size, 'bytes', '\ncreate at:', s.birthtime,
          '\nmodify at:', s.mtime, '\non:', s.dev);
    }
    next();
  });
};

helper.rm = 'filename';
cmd.rm = function rm(av, next) {
  needav(av[1]);
  ckfs().unlink(fix(av[1]), function(e) {
    if (e) log(e.message);
    next();
  });
};

helper.mv = 'sourcefile destfile';
cmd.mv = function mv(av, next) {
  var s = fix(needav(av[1]));
  var d = fix(needav(av[2]));
  ckfs().rename(s, d, function(e) {
    if (e) log(e.message);
    next();
  });
};

helper.mkdir = 'dirname';
cmd.mkdir = function mkdir(av, next) {
  ckfs().mkdir(fix(needav(av[1])), function(e) {
    if (e) log(e.message);
    next();
  });
};


helper.cp = 'sourcefile destfile';
cmd.cp = function cp(av, next) {
  var rf = fix(needav(av[1]));
  var wf = fix(needav(av[2]));
  ckfs().exists(wf, function(ex) {
    if (ex) {
      log('fail:', wf, 'is exists');
      return next();
    }
    var r = cmd._openReadStream(rf);
    var w = cmd._openWriteStream(wf);
    r.pipe(w);
    r.on('error', function(err) {
      log(err.message);
    });
    r.on('end', function() {
      log('success');
      next();
    });
  });
};


cmd._openWriteStream = function(file) {
  return ckfs().createWriteStream(fix(file));
};


cmd._openReadStream = function(file) {
  return ckfs().createReadStream(fix(file));
};


function update_prompt() {
  if (isNaN(hdidx)) {
    hdidx = '[need-use-harddisk]';
  }
  if (isNaN(dvidx)) {
    dvidx = '[need-use-driver]';
  }
  _setPrefix('?' + dvidx + ':' + hdidx + ':' + root);
}

function fix(path) {
  if (plib.isAbsolute(path)) {
    return plib.normalize(path);
  }
  return plib.join(root, path);
}

function ckdr() {
  if (!driver) throw new Error('fail: (open) driver first');
  return driver;
}

function ckfs() {
  if (!fs) throw new Error('fail: (use) fs first');
  return fs;
}

// 调用 needav 的函数必须有函数名
function needav(a) {
  if (!a) {
    cmd.help([0, needav.caller.name], function() {
      throw new Error('fail: need arguments');
    });
  }
  return a;
}

}
