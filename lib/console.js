var impl    = require('./cmd-impl.js');
var driver  = require('./driver.js');
var clib    = require('configuration-lib');
var lofs    = require('fs');

var prefix  = 'nul';
var command = {};
var waiti   = false;
var stdout  = process.stdout;
var stdin   = process.stdin;

// fix stdout throw error when end()
stdout.end = function() {};

log('\n\n[ Net File System with cassandra Console ]\n');

clib.wait_init(function() {
  var conf = clib.load();
  conf.localdir = clib.nodeconf + '/nfs-local-data';
  clib.mkdir(conf.localdir);
  impl(command, setPrefix, console.log,
    driver, getStdin, getStdout, conf, openLocalFS);
  connectConsole();
});


function connectConsole() {
  if (waiti) return;
  waiti   = true;
  stdout  = process.stdout;
  stdin   = process.stdin;

  console.log();
  log(prefix, '> ');
  stdin.on('data', function(chunk) {
    waiti = false;
    stdin.removeAllListeners('data');
    readConsole(chunk);
  });
  stdin.resume();
}


function readConsole(chunk) {
  try {
    var argv = parseCmd(chunk);
    if (!argv[0]) throw new Error('input a command');

    var cmd = command[argv[0]];
    if (!cmd) {
      throw new Error('invalid command ' + argv[0]);
    } else if (argv[0] != 'test'){
      if (argv.inputfile) {
        stdin = command._openReadStream(argv.inputfile);
      }
      if (argv.outfile) {
        stdout = command._openWriteStream(argv.outfile);
      }
    }
    
    cmd(argv, function(err) {
      if (err) {
        if (err.message) {
          console.log(err.message);
        } else {
          console.log(err);
        }
      }
      connectConsole();
    });
  } catch(e) {
    console.error(e.message);
    connectConsole();
  }
}


process.on('uncaughtException', function(err)  {
  console.error(err);
  connectConsole();
});


function setPrefix(p) {
  if (p) {
    prefix = p.toString();
  }
}


function getStdin() {
  return stdin;
}


function getStdout() {
  return stdout;
}


function parseCmd(chunk) {
  var S_BEGIN     = 0,
      S_INWORD    = 1,
      S_SINGLE    = 2,
      S_DOUBLE    = 3,
      S_QM_ED     = 4,
      S_OUTPUT    = 5,
      S_OUTPUT_B  = 6,
      S_INPUT     = 7,
      S_INPUT_B   = 8;

  var s     = chunk.toString();
  var av    = [];
  var st    = 0;
  var state = 0;

  for (var i=0, e=s.length; i<e; ++i) {
    // console.log(s[i], i,  state);
    if (s[i] == '\r') continue;
    if (s[i] == ' ' || s[i] == '\t' || s[i] == '\n') {
      if (state == S_QM_ED) {
        state = S_BEGIN;
      } else if (endsome(i)) {
        state = S_BEGIN;
      }
    }
    else if (s[i] == "'") {
      if (state == S_BEGIN) {
        st = i+1;
        state = S_SINGLE;
        continue;
      }
      else if (state == S_SINGLE) {
        closeParm(i);
        state = S_QM_ED;
      }
      else {
        invalid(i);
      }
    }
    else if (s[i] == '"') {
      if (state == S_BEGIN) {
        st = i+1;
        state = S_DOUBLE;
        continue;
      }
      else if (state == S_DOUBLE) {
        closeParm(i);
        state = S_QM_ED;
      }
      else {
        invalid(i);
      }
    }
    else if (s[i] == '>') {
      if (endsome(i)) {
        // donothing
      }
      else if (state != S_BEGIN) {
        invalid(i);
      }
      state = S_OUTPUT_B;
    }
    else if (s[i] == '<') {
      if (endsome(i)) {
        // donothing
      }
      else if (state != S_BEGIN) {
        invalid(i);
      }
      state = S_INPUT_B;
    }
    else if (s[i] == '|') {
      throw new Error('unsupport | pipe');
    }
    else {
      if (state == S_QM_ED) invalid(i);
      else if (state == S_BEGIN) {
        state = S_INWORD;
        st = i;
      }
      else if (state == S_OUTPUT_B) {
        state = S_OUTPUT;
        st = i;
      }
      else if (state == S_INPUT_B) {
        state = S_INPUT;
        st = i;
      }
    }
  }

  if (state != 0) {
    invalid(s.length-1);
  }

  function endsome(i) {
    if (state == S_INWORD) {
      closeParm(i);
      return true;
    }
    else if (state == S_INPUT) {
      setExfile(i, 'inputfile');
      return true;
    }
    else if (state == S_OUTPUT) {
      setExfile(i, 'outfile');
      return true;
    }
    return false;
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

  function setExfile(i, name) {
    if (av[name]) {
      invalid(i);
    }
    av[name] = s.substring(st, i).trim();
    st = i + 1;
    if (!av[name]) {
      invalid(i);
    }
  }

  return av;
}


function log(a) {
  if (a) process.stdout.write('' + a);
  for (var i=1, e=arguments.length; i<e; ++i) {
    process.stdout.write(' ' + arguments[i]);
  }
}


function openLocalFS() {
  // throw new Error('unsupport local fs');
  return lofs;
}
