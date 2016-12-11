var plib   = require('path');
var stream = require('stream');
var cqld   = require('./cql.js');
var Jszip  = require('jszip');


module.exports = function createImpl(
  cmd, _setPrefix, log, driverlib, stdin, stdout, config, openLocalFS) {

// 不要直接引用, 使用 ckfs() 和 ckdr()
var fs, driver, drcf;
var hdids, hdinf;
var hdidx, hdid;
var root;
var stack  = [];
var helper = {};

update_prompt();


cmd.help = function(argv, next) {
  log();
  var sub = argv[1];
  if (!sub) {
    log('help       -- print help infomation');
    log('exit, quit -- quit the console');
    log('open       -- open remote driver');
    log('use        -- open hard disk with hdid from current env');
    log('close      -- close current env drver/hd/fs');
    log('driver     -- show current driver connect info');
    log('list       -- list all hard disk and index');
    log('update     -- update hard dist list');
    log('mkhd       -- crate hard disk');
    log('rmhd       -- remove hard disk');
    log('al         -- show all driver/hard-list/fs from stack');
    log('pop        -- clear current env and pop stack to this');
    log('push       -- put all current env driver/hd/fs to stack and clear this');
    log();
    log('ls         -- list dir files');
    log('cd         -- change work directory');
    log('cat        -- concat input and output');
    log('stat       -- show file state');
    log('rm         -- remove file');
    log('mv         -- move file');
    log('mkdir      -- make dir');
    log('cp         -- copy file content');
    log('diff       -- Compare bin FILES different');
    log();
    log('zip        -- compress file/dir');
    log('uzip       -- uncompress file to dir');
    log();
    log('we can use path "?stack-index:/path" \
      \n to ref file from another driver/hard-disk');
    log('\nwe can use path "!/path" to ref local file system');
  } else if (helper[sub]) {
    log('Usage: ', sub, helper[sub]);
  } else {
    log('Command:', sub, 'no help');
  }
  next();
};

cmd.quit = cmd.exit = function() {
  process.exit(0);
};

helper.test = '[do nothing]';
cmd.test = function test(argv, next) {
  log('ARGV:', argv);
  next();
};

helper.al = '(no arguments)';
cmd.al = function(av, next) {
  if (stack.length < 1)
    throw new Error('stack is empty');

  log('\n[index] [info]');
  stack.forEach(function(s, i) {
    log('------- ------------------------------------------------------');
    log('', i,
        '\t   opened hd id :', s.hdid,
      '\n\t        hd note :', s.hdinf[s.hdidx].note,
      '\n\t hd create time :', new Date(s.hdinf[s.hdidx].create_tm.toNumber()),
      '\n\t    working dir :', s.root,
      '\n\tcassandra hosts :', s.drcf.cassandra.contactPoints,
      '\n\t  cass keyspace :', s.drcf.cassandra.keyspace,
      '\n\t     redis host :', s.drcf.redis_conf.host,
      '\n\t     redis port :', s.drcf.redis_conf.port);
  });
  next();
};

helper.pop = '(no arguments)';
cmd.pop = function(av, next) {
  if (driver) {
    log('current dirver must closed first for pop');
    return next();
  }
  var s = stack.pop();
  if (!s) {
    log('stack has not anything');
    return next();
  }
  driver = s.driver;
  fs     = s.fs;
  hdids  = s.hdids;
  hdinf  = s.hdinf;
  hdidx  = s.hdidx;
  hdid   = s.hdid;
  drcf   = s.drcf;
  root   = s.root;
  log('pop finished');
  update_prompt();
  next();
};

helper.push = '(no arguments)';
cmd.push = function(av, next) {
  if (!driver) return log('no driver for push, open it first'), next();
  if (!fs) return log('no fs for push, use it first'), next();
  stack.push({
    driver : driver,
    fs     : fs,
    hdids  : hdids,
    hdinf  : hdinf,
    hdidx  : hdidx,
    hdid   : hdid,
    drcf   : drcf,
    root   : root,
  });
  _clear();
  log('push finished');
  update_prompt();
  next();
};

helper.open = 'cassandra-host[:cass-port] cass-keyspace \
redis-host redis-port redis-db-num \
\n\nif port = 0 will use default port,\
\nif redis-host = 0 while use same port with cassandra-host';
cmd.open = function open(av, next) {
  if (driver) {
    log('open new driver must close/push current driver first');
    return next();
  }

  if (av.length == 1) {
    log('use default config to connect');
    drcf = config;
  }
  else if (av.length == 6) {
    drcf = {
      cassandra : {
        contactPoints : [ av[1] ],
        keyspace      : av[2],
      },
      redis_conf : {
        host : av[3] || av[1],
        port : av[4] || 6379,
        db   : av[5],
      },
    };
  }
  else {
    log('bad arguments, Usage: \nopen', helper.open);
    return next();
  }

  driverlib.open_driver(drcf, function(err, _driver) {
    if (err) return log(err.message), next();
    driver = _driver;

    cmd.update(av, function() {
      log('nfs driver opend');
      update_prompt();
      next();
    });
  });
};

helper.close = '(no arguments)';
cmd.close = function close(av, next) {
  if (!driver) {
    log('fail: no driver has close');
    return next();
  }
  ckdr().end();
  _clear();
  log('driver/hd closed, env clear');
  update_prompt();
  next();
};

helper.driver = '(no arguments)';
cmd.driver = function _driver(av, next) {
  ckdr();
  log('cassandra:\n\thosts:', drcf.cassandra.contactPoints,
      '\n\tkeyspace:', drcf.cassandra.keyspace,
      '\n\nredis:\n\thost:', drcf.redis_conf.host,
      '\n\tport:', drcf.redis_conf.port);
  next();
};

helper.use = '{hd-index | hd-id}';
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

  if (hdid) {
    if (id == hdid) {
      log('the hd is current used');
      return next();
    } else {
      fs.quit();
    }
  }

  ckdr().open_fs(id, function(err, xfs) {
    if (err) log(err.message);
    else {
      fs    = xfs;
      hdid  = id;
      hdidx = i;
      root  = '\\';
      log('fs opend');
      update_prompt();
    }
    next();
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

helper.rmhd = '{hdid | hd-index}'
cmd.rmhd = function(av, next) {
  var d = ckdr();
  var id = hdids[ av[1] ] || av[1];
  if (id == hdid) {
    throw new Error('cannot remove using hd in current');
  }

  stack.forEach(function(s, i) {
    if (id == s.hdid) {
      throw new Error('cannot rmove using hd in stack');
    }
  });

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

helper.ls = '[dirname]'
cmd.ls = function(av, next) {
  var fp = gtfp(av[1] || root || '/');
  fp.fs.readdir(fp.path, function(err, files) {
    if (err) {
      log(err.message);
      return next();
    }
    var out = [];
    var i =-1;
    nfile();

    function nfile() {
      if (++i < files.length) {
        fp.fs.stat(plib.join(fp.path, files[i]), function(e, s) {
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
  ckfs();
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
    if (e) console.error(e.message);
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
  var fp = gtfp(needav(av[1]));
  fp.fs.stat(fp.path, function(e, s) {
    if (e) log(e.message);
    else {
      log(fp.path, '\t', s.size, 'bytes', '\ncreate at:', s.birthtime,
          '\nmodify at:', s.mtime, '\non:', s.dev);
    }
    next();
  });
};

helper.rm = 'filename';
cmd.rm = function rm(av, next) {
  var fp = gtfp(needav(av[1]));
  fp.fs.unlink(fp.path, function(e) {
    if (e) log(e.message);
    next();
  });
};

helper.mv = 'sourcefile destfile';
cmd.mv = function mv(av, next) {
  var s = gtfp(needav(av[1]));
  var d = gtfp(needav(av[2]));
  if (s.fs !== d.fs)
    throw new Error('cannot move file between different fs');

  s.fs.rename(s.path, d.path, function(e) {
    if (e) log(e.message);
    next();
  });
};

helper.mkdir = 'dirname';
cmd.mkdir = function mkdir(av, next) {
  var fp = gtfp(needav(av[1]));
  fp.fs.mkdir(fp.path, function(e) {
    if (e) log(e.message);
    next();
  });
};


helper.cp = 'sourcefile destfile';
cmd.cp = function cp(av, next) {
  var rf = gtfp(needav(av[1]));
  var wf = gtfp(needav(av[2]));

  wf.fs.exists(wf.path, function(ex) {
    if (ex) {
      log('fail:', wf.path, 'is exists');
      return next();
    }
    var r = cmd._openReadStream(rf);
    var w = cmd._openWriteStream(wf);
    r.pipe(w);
    r.on('error', function(err) {
      log('cannot read sourcefile', err.message);
      next();
    });
    w.on('error', function(e) {
      log('cannot write destfile', e.message);
      next();
    });
    r.on('end', function() {
      log('success');
      next();
    });
  });
};

helper.diff = 'file1 file2';
cmd.diff = function diff(av, next) {
  var f1 = gtfp(needav(av[1]));
  var f2 = gtfp(needav(av[2]));
  f1.fs.open(f1.path, 'r', function(err, fd1) {
    if (err) {
      log(err.message);
      return next();
    }
    f2.fs.open(f2.path, 'r', function(err, fd2) {
      if (err) {
        log(err.message);
        f1.fs.close(fd1);
        return next();
      }

      var bs   = cqld.BLOCK_SIZE;
      var buf1 = Buffer.alloc(bs);
      var buf2 = Buffer.alloc(bs);
      var pos  = 0, ch;
      var diffc = 0;
      log('[addr]\t[different]');
      ndata();

      function ndata() {
        f1.fs.read(fd1, buf1, 0, bs, pos, function(err, len1) {
          if (err) return over(err);
          f2.fs.read(fd2, buf2, 0, bs, pos, function(err, len2) {
            if (err) return over(err);

            var i = 0, e = Math.min(len1, len2);
            while (i<e) {
              if (buf1[i] != buf2[i]) {
                log('', '0x' + i.toString(16), '\t',
                  buf1[i].toString(16), buf2[i].toString(16));
                ++diffc;
              }
              ++i;
            }
            if (len1 < bs || len2 < bs) {
              log();
              if (len1 != len2) {
                log('different size');
                ++diffc;
              }
              over();
            } else {
              pos += e;
              ndata();
            }
          });
        });
      }

      function over(err) {
        if (err) log(err.message);
        if (diffc) {
          log('has', diffc, 'different');
        }
        f1.fs.close(fd1, function() {
          f2.fs.close(fd2, next);
        });
      }
    });
  });
};

helper.zip = '{sourcefile | sourcefiledir} destzipfile';
cmd.zip = function zip(av, next) {
  var sf = gtfp(needav(av[1]));
  var df = gtfp(needav(av[2]));
  var base = plib.dirname(sf.path);
  var name = plib.basename(sf.path);
  var zip  = new Jszip();

  df.fs.exists(df.path, function(exist) {
    if (exist) return next('fail: dest file is exist: ' + df.path);
    begin();
  });

  function begin() {
    walkdir(sf.fs, base, name, function(err, list) {
      if (err) return next(err);
      var i = -1;
      each();
      function each() {
        if (++i < list.length) {
          var f = list[i];
          if (f.type == 'file') {
            sf.fs.readFile(f.complete, function(err, buf) {
              if (err) return next(err);
              zip.file(f.name, buf);
              log('file\t', f.name, '\t', buf.length, 'bytes');
              each();
            });
          } else if (f.type == 'dir') {
            zip.folder(f.name);
            log('dir\t', f.name);
            each();
          } else {
            log('UNKNOW type', f.type, f.name);
            each();
          }
        } else {
          writezip();
        }
      }
    });
  }

  function writezip() {
    zip
    .generateNodeStream({ type:'nodebuffer',streamFiles:true })
    .pipe(df.fs.createWriteStream(df.path))
    .on('finish', function () {
        console.log("out.zip written.");
        next();
    });
  }
};

helper.uzip = 'sourcezipfile destdir';
cmd.uzip = function uzip(av, next) {
  var sf = gtfp(needav(av[1]));
  var df = gtfp(needav(av[2]));

  sf.fs.readFile(sf.path, function(err, buffer) {
    if (err) return next(err);
    var ens = [], i = -1;
    var zip = new Jszip();
    zip.loadAsync(buffer).then(bufferLoaded);

    function bufferLoaded(obj) {
      for (var n in obj.files) {
        ens.push(obj.files[n]);
      }
      eachEnt();
    }

    function eachEnt() {
      if (++i < ens.length) {
        var ent = ens[i];
        var fname = plib.join(df.path, ent.name);

        if (ent.dir) {
          df.fs.mkdir(fname, function(err) {
            if (err) return next(err);
            log('dir\t', fname);
            eachEnt();
          });
        } else {
          ent.async('nodebuffer').then(function(buf) {
            df.fs.writeFile(fname, buf, 'wx', function(err) {
              if (err) return next(err);
              log('file\t', fname);
              eachEnt();
            });
          });
        }
      } else {
        log('success');
        next();
      }
    }
  });
};


cmd._openWriteStream = function(file) {
  var fp = gtfp(file);
  return fp.fs.createWriteStream(fp.path);
};


cmd._openReadStream = function(file) {
  var fp = gtfp(file);
  return fp.fs.createReadStream(fp.path);
};


function update_prompt() {
  var p;
  if (!driver) {
    p = 'NODRIVER';
  } else if (!fs) {
    p = 'NOHD';
  } else {
    p = root;
  }
  _setPrefix(p);
}

// 修正传入的路径
function fix(path) {
  if (!path) throw new Error('path must string (fix)');
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

//
// 解析路径返回连接到路径的 fp 对象, 支持 stack/local
// return { fs, path }
//
function gtfp(path) {
  var rfs;
  if (!path)
    throw new Error('path must string (gtfs)');

  // path 是一个 fp 对象, 直接返回
  if (path.path && path.fs) {
    return path;
  }

  if (path[0] == '!') {
    rfs = openLocalFS();
    path = path.substr(1);

    if (plib.isAbsolute(path)) {
      path = plib.normalize(path);
    } else {
      path = plib.join(config.localdir, path);
    }
  }
  else if (path[0] == '?') {
    var ed_idx = path.indexOf(':', 1);
    if (ed_idx < 0)
      throw new Error('fail path ' + path
        + ' the stack path is "?stack-index:/somepath"');

    var stackidx = parseInt( path.substring(1, ed_idx) );
    if (isNaN(stackidx))
      throw new Error('fail stack index "' + stackidx + '" from ' + paht);

    var st = stack[stackidx];
    if (st == null || st.fs == null) {
      throw new Error('stack index ' + stackidx
        + ' is invalid then ' + (stack.length-1));
    }

    rfs = st.fs;
    path = path.substr(stackidx + 1);

    if (plib.isAbsolute(path)) {
      path = plib.normalize(path);
    } else {
      path = plib.join(st.root, path);
    }
  }
  else {
    rfs  = ckfs();
    path = fix(path);
  }
  return { fs : rfs, path : path };
}

//
// 清掉所有变量, 之前必须释放资源否则内存泄漏
//
function _clear() {
  hdids = hdinf = driver = fs = null;
  hdidx = hdid  = drcf   = null;
  root  = null;
}

//
// 调用 needav 的函数必须有函数名
//
function needav(a) {
  if (!a) {
    cmd.help([0, needav.caller.name], function() {
      throw new Error('fail: need arguments');
    });
  }
  return a;
}

//
// 遍历 _rdir/_name 并从 _name 开始, 每个文件/目录
// 回调 cb , 返回的路径都是基于 _dir 的相对路径.
// cb : Function(err, infos)
//  infos : [ { name: 相对于_rdir的路径, type: 'file/dir',
//   complete: 文件的完整路径, size: 如果是文件则设置为文件长度 } ]
//
function walkdir(_fs, _rdir, _name, cb) {
  // dirs 是相对于 _rdis 的目录
  var dirs = [];
  var rets = [];
  type(_rdir, _name, function(err) {
    if (err) return cb(err);
    cb(null, rets);
  });

  function type(dir, name, over) {
    var compf = plib.join(dir, name);
    var abs = dirs.join('/');
    _fs.stat(compf, function(err, st) {
      if (err) return over(err);
      if (st.isDirectory()) {
        dirs.push(name);
        rets.push({
          name : plib.join(abs, name),
          type : 'dir',
          size : st.size,
          complete : compf,
        });
        eachdir(compf, over);
      } else {
        rets.push({
          name : plib.join(abs, name),
          type : 'file',
          complete : compf,
        });
        over();
      }
    });
  }

  function eachdir(dirname, over) {
    _fs.readdir(dirname, function(err, list) {
      if (err) return over(err);
      var i = -1;
      eachfile();
      function eachfile() {
        if (++i < list.length) {
          type(dirname, list[i], eachfile);
        } else {
          dirs.pop();
          over();
        }
      }
    });
  }
}

}
