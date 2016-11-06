require('./tdriver.js').on('finish', function() {
  require('./tfs.js').on('finish', function() {
    require('./fd.js').on('finish', function() {
      require('./link.js')
    });
  });
});
