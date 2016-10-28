
module.exports = {
  do_cql : do_cql,
};


function do_cql(client, cql_arr, next) {
  var i = -1;
  var ress = [];
  _do();
  
  function _do() {
    if (++i<cql_arr.length) {
      var c = cql_arr[i];
      console.log('\nDO: [\n', c);
      client.execute(c, [], function(err, res) {
        ress.push(res);
        if (err) {
          console.log('] fail');
          next(err, ress);
          return;
        }
        console.log('] ok');
        _do();
      });
    } else {
      next(null, ress);
    }
  }
}