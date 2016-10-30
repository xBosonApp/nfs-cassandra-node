

require('./test-base.js')(tdriver_main);


function tdriver_main(driver) {
  var hdinfo;
  var len = 0;

  var d = {
    create_hd: function(test) {
      driver.create('test1', function(err, info) {
        hdinfo = info;
        test.assert(err);
        test.end();
      });
    },

    create_hd_nodelete: function(test) {
      if (!test.wait('list_hd')) return;
      if (len >= 2) {
        test.end();
        return;
      }
      driver.create('hd1', function(err, info) {
        test.assert(err);
        test.end();
        require('fs').writeFileSync(__dirname + '/driver-id', info.hd_id);
      });
    },

    delete_hd: function(test) {
      if (!test.wait('list_hd', 'create_hd_nodelete',
        'create_hd', 'hd_state')) return;
      driver.delete(hdinfo.hd_id, function(err) {
        test.assert(err);
        test.end();
      });
    },

    list_hd: function(test) {
      if (!test.wait('create_hd')) return;
      driver.list(function(err, list) {
        len = list.length;
        test.log('hd list:', list);
        test.assert(err);
        test.end();
      });
    },

    hd_state: function(test) {
      if (!test.wait('list_hd')) return;
      driver.state(hdinfo.hd_id, function(err, info) {
        test.log('hd state:', info);
        test.assert(err);
        test.end();
      });
    }
  };
  return d;
}
