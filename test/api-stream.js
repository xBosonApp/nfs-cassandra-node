// 验证 nodejs stream 工作原理

var fs = require('fs');
var stream = require('stream');


//t1(0, 0, 1);
t1(0, 1, 0);


//
// 从文件读入, 测试写出流工作方式
//
function t1(writeError, notusefile, readError) {
  var w = new stream.Writable();
  var r;
  
  if (!notusefile) {
    if (readError) {
      // 如果输入流出错, 并不会关闭 w, 必须手动关闭 w
      r = fs.createReadStream(__dirname + '/__.js');
      r.pipe(w);
    } else {
      r = fs.createReadStream(__dirname + '/ymtest.js');
      r.pipe(w);
    }
    r.on('error', function(e) {});
  } else {
    setTimeout(function() {
      w.write('x');
      w.end();
      // 超过一次的 end 被忽略, 不会有错误
      w.end();
    });
  }
  
  w._write = function(chunk, encoding, callback) {
    console.log('write chunk');
    if (writeError) callback(new Error('write test'));
    else callback();
  }
  
  // 不是所有流都有这个消息
  w.on('close', function() {
    console.log('write close');
  });

  // 不是所有流都有这个消息
  w.on('open', function(fd) {});

  w.on('drain', function() {
    console.log('write drain');
  });

  // 如果出错, 被触发, 且不会 finish
  w.on('error', function(e) {
    console.log('write error', e.message);
  });

  // 调用 w.end() 且没有出错, 则 finish 被触发
  w.on('finish', function() {
    console.log('write finish');
  });

  // !! Writable 没有 end 事件
  w.on('end', function() {
    console.log('write end');
  });
}