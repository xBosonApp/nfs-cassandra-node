// 默认配置文件, 集成了多个系统的默认配置
module.exports = {
  port    : 80,
  cluster : false,

  logger : {
    logLevel : 'ALL',
    log_dir  : 'logs',
  },

  cassandra : {
    contactPoints : ['192.168.1.104'],
    keyspace      : 'fs',
    debug_log     : true,
  },

  redis_conf: {
	  host: "localhost",
	  port: "6379",
	  db: 2,
	  options: {
			// redis options see `redis README`
	    enable_offline_queue: true,
	    auth_pass: null
	  },
	  defaultExpiration: 7200
	}

};
