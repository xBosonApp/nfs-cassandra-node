// 默认配置文件, 集成了多个系统的默认配置
module.exports = {
  port    : 80,
  cluster : false,

  logger : {
    logLevel : 'ALL',
    log_dir  : 'logs',
  },

  cassandra : {
    contactPoints : ['192.168.1.102'],
    keyspace      : 'fs',
    debug_log     : true,
  },
};
