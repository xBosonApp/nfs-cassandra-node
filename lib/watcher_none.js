var Event = require('events');

//
// 默认文件监听器实现, 什么都不做
//
module.exports = function create() {

  return {
    // Function(hdid, filename, _options)
    watch  : watch,
    // Function(hdid, filename, type) 发布文件修改消息, filename 文件名, type 操作类型.
    change : change,
    // Function() 关闭监听器
    end    : end,
  };

  function end() {
    console.log('Watcher /// end');
  }

  function watch(hdid, filename, _options) {
    console.log('Watcher /// watch', hdid, filename, _options);
    var w = new Event();
    w.close = function() {
      w.removeAllListeners();
    };
    return w;
  }

  function change(hdid, filename, type) {
    console.log('Watcher /// change', hdid, filename, type);
  }
}
