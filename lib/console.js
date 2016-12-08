var impl    = require('./cmd-impl.js');
var driver  = require('./driver.js');

var prefix  = '0';
var command = {};
var log     = createLog();
var waiti   = false;

log('\n\n[ Net File System with cassandra Console ]\n');
impl(command, setPrefix, console.log, driver);
connectConsole();


function connectConsole() {
  if (waiti) return;
  console.log();
  log(prefix, '> ');
  waiti = true;
  var sin = process.stdin;
  sin.on('data', function(chunk) {
    waiti = false;
    sin.removeAllListeners('data');
    readConsole(chunk);
  });
  sin.resume();
}


function readConsole(chunk) {
  try {
    var argv = parseCmd(chunk);
    if (!argv[0]) throw new Error('input a command');

    var cmd = command[argv[0]];
    if (!cmd) {
      throw new Error('invalid command ' + argv[0]);
    } else {
      cmd(argv, connectConsole);
    }
  } catch(e) {
    console.log(e.message);
    connectConsole();
  }
}


process.on('uncaughtException', function(err)  {
  console.log(err);
  connectConsole();
});


function setPrefix(p) {
  if (p) {
    prefix = p.toString();
  }
}


function parseCmd(chunk) {
  var s = chunk.toString();
  var av = [];
  var st = 0;
  var state = 0;

  for (var i=0, e=s.length; i<e; ++i) {
    // console.log(s[i], i,  state);
    if (s[i] == '\r') continue;
    if (s[i] == ' ' || s[i] == '\t' || s[i] == '\n') {
      if (state == 4) {
        state = 0;
      }
      if (state == 1) {
        state = 0;
        closeParm(i);
      }
    }
    else if (s[i] == "'") {
      if (state == 1 || state == 3 || state == 4) {
        invalid(i);
      }
      if (state == 0) {
        st = i+1;
        state = 2;
        continue;
      }
      if (state == 2) {
        closeParm(i);
        state = 4;
      }
    }
    else if (s[i] == '"') {
      if (state == 1 || state == 2 || state == 4) {
        invalid(i);
      }
      if (state == 0) {
        st = i+1;
        state = 3;
        continue;
      }
      if (state == 3) {
        closeParm(i);
        state = 4;
      }
    }
    else if (s[i] == '>') {
      if (state != 0) {
        invalid(i);
      }
      av.outfile = s.substr(i+1).trim();
      st = s.length;
      if (!av.outfile) {
        invalid(i);
      }
      break;
    }
    else if (s[i] == '<') {
      throw new Error('unsupport < input');
    }
    else if (s[i] == '|') {
      throw new Error('unsupport | pipe');
    }
    else {
      if (state == 4) invalid(i);
      if (state == 0) {
        state = 1;
        st = i;
      }
    }
  }
  if (state != 0) {
    invalid(s.length-2);
  }
  if (st < s.length) {
    closeParm(s.length);
  }

  function invalid(i) {
    var sp = [];
    sp.length = i + prefix.length + 3;
    console.log(sp.join(' '), '^');
    throw new Error('invalid command');
  }

  function closeParm(ed) {
    var p = s.substring(st, ed).trim();
    p && av.push(p);
    st = ed + 1;
  }
  return av;
}


function createLog() {
  var sout = process.stdout;
  return function(a) {
    if (a) sout.write('' + a);
    for (var i=1, e=arguments.length; i<e; ++i) {
      sout.write(' ' + arguments[i]);
    }
  };
}
