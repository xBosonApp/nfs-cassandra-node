var plib   = require('path');
var stream = require('stream');

module.exports = create;

function create(cmd, setPrefix, log, driverlib) {
  var fs, driver;
  var hdid;
  var root = '\\';
  var cid;

  cmd.quit = cmd.exit = function() {
    process.exit(0);
  };

  cmd.help = function(argv, next) {
    log();
    log('help       -- print help infomation');
    log('exit, quit -- quit the console');
    log('open       -- open driver');
    log('list       -- list all hard disk');
    log('update     -- update hard dist list');
    log('use        -- open hard disk with hdid');
    log('ls         -- list dir files');
    log('cd         -- change work directory');
    log('cat        -- concat input and output');
    log('stat       -- show file state');
    log('rm         -- remove file');
    next();
  };

  cmd.open = function(argv, next) {
    log('use default config to connect');
    driverlib.open_driver(function(err, _driver) {
      if (err) return log(err.message), next();
      driver = _driver;
      cmd.update(argv, function() {
        log('nfs driver opend');
        setPrefix('1');
        next();
      });
    });
  };

  cmd.update = function(argv, next) {
    driver.list(function(err, ids) {
      if (err) log(err.message);
      else {
        hdid = ids;
        log('update hd list');
      }
      next();
    });
  };

  cmd.list = function(a, next) {
    ckdr();
    var size = hdid.length;
    hdid.forEach(function(id, index) {
      driver.state(id, function(err, s) {
        if (err) return log(err.message);
        log('\n', index, s);
        if (--size <= 0)
          next();
      });
    });
  };

  cmd.use = function(a, next) {
    if (!hdid) ckdr();
    var id = hdid[a[1] || 0] || a[1];
    if (!a[1]) log('use default hd');
    if (fs) {
      fs.quit();
    }
    ckdr().open_fs(id, function(err, xfs) {
      if (err) log(err.message);
      else {
        fs = xfs;
        log('fs opend');
        cid = id;
        setPrefix('HD ' + cid + ' ' + root);
      }
      next();
    });
  };

  cmd.ls = function(av, next) {
    ckfs().readdir(root, function(err, files) {
      log(files.join('\t'));
      next();
    });
  };

  cmd.cd = function(av, next) {
    ckfs();
    if (!av[1]) return log('Usage: cd [dir]'), next();
    if (av[1] == '/' || av[1] == '\\') {
      root = '\\';
      setPrefix('HD ' + cid + ' ' + root);
      return next();
    }
    var cdir = plib.join(root, av[1]);

    fs.stat(cdir, function(err, s) {
      if (err) log(err.message);
      else if (s.isDirectory()) {
        root = cdir;
        setPrefix('HD ' + cid + ' ' + root);
      } else {
        log('fail:', cdir, 'is not dir');
      }
      next();
    });
  };

  cmd.cat = function(av, next) {
    var r, w, t;
    if (av[1]) {
      r = ckfs().createReadStream(plib.join(root, av[1]));
      w = process.stdout;
    } else {
      r = process.stdin;
      w = av.outfile
        ? ckfs().createWriteStream(plib.join(root, av.outfile))
        : process.stdout;
      _bindover(r);
    }
    r.pipe(w);
    r.on('end', _close);
    r.on('error', _close);
    w.on('error', _close);
    function _close(e) {
      if (e) log(e.message);
      r.removeAllListeners('data');
      setTimeout(next, 1);
    }
    function _bindover(r, t) {
      r.on('data', function(chunk) {
        var ch = chunk[0];
        if (ch == 0x0d || ch == 0x0a) {
          r.unpipe(w);
          setTimeout(function() { w.end(); }, 100);
          _close();
        }
      });
    }
  };

  cmd.stat = function(av, next) {
    ckfs().stat(plib.join(root, av[1]), function(e, s) {
      if (e) log(e.message);
      else log(s);
      next();
    });
  };

  cmd.rm = function(av, next) {
    ckfs().unlink(plib.join(root, av[1]), function(e) {
      if (e) log(e.message);
      next();
    });
  };

  function ckdr() {
    if (!driver) throw new Error('(open) driver first');
    return driver;
  }

  function ckfs() {
    if (!fs) throw new Error('(use) fs first');
    return fs;
  }
}
