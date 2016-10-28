var cassandra = require('cassandra-driver');
var conlib    = require('configuration-lib');
var tool      = require('./lib/tool.js');


function get_cql(ks) {
return [
  "CREATE KEYSPACE IF NOT EXISTS " + ks + " WITH REPLICATION =\
      { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };",
      
  "use " + ks,
  
  "CREATE TABLE IF NOT EXISTS driver_main (\
      id        uuid PRIMARY KEY, \
      create_tm bigint, \
  );",
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