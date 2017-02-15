var cassandra = require('cassandra-driver');
var conlib    = require('configuration-lib');
var tool      = require('./lib/tool.js');

//
// 功能框架
//
function get_cql(ks) {
return [
  "CREATE KEYSPACE IF NOT EXISTS " + ks + " WITH REPLICATION = \
      { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };",

  "use " + ks,
];
}


conlib.wait_init(function() {
  var conf = conlib.load();
  var ks = conf.cassandra.keyspace;
  delete conf.cassandra.keyspace;

  var client = new cassandra.Client(conf.cassandra);
  tool.do_cql(client, get_cql(ks), function(err, res) {
    if (err) {
      process.exit(1);
    } else {
      console.log('install success');
      process.exit();
    }
  });
});
