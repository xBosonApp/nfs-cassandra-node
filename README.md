# Net file system (fs) api

fs api doc version on nodejs <=0.12.x

每个 cassandra 的 keyspace 可以保存多个 driver; 把 driver 想象成硬盘,  
每个硬盘都格式化成 ext4 格式, 可以在硬盘上打开文件系统(fs)进行操作.
应该给 fs 一个独立的 keyspace.

由于 NFS 可以有多个用户同时修改文件, 读取文件状态后进行操作是不安全的,
由于没有文件锁, 文件内容和长度随时可以改变, cassandra 操作有延迟, 写入
后立即读取未必是最新的状态

0.1.x 实现不支持用户权限/组权限, 不支持(软/硬)符号链接.


# install

`npm install fs-cassandra-lib --save`


# Usage

所有带有 Sync 结尾的同步式函数都不支持, 调用时会抛出异常.

```js
var cassandra = require('cassandra-driver');
var fs_cass = require('fs-cassandra');

var client = new cassandra.Client({ contactPoints: ['h1', 'h2'], keyspace: 'ks1'});
var driver = fs_cass.open_driver(client);


// 与 nodejs 的 fs 对象相同
driver.open_fs('driver-id', function(err, fs) {
  fs.quit();
});
```

# Api

#### var fs_cass = require('fs-cassandra')

  导入库

#### driver = fs_cass.open_driver(cassandra_client)

  打开一个驱动, 参数是已经链接的 cassandra 客户端, 之后所有操作都是基于这个连接的.

#### driver.create(note, cb)

  使用驱动创建一块硬盘, 如果成功将返回驱动 id, note 是对硬盘的描述字符串,
  cb => (err, info), info = { hd_id -- 硬盘id }

#### driver.delete(hd_id, cb)

  删除一块硬盘, 如果硬盘已经被打开, 这些操作将会出错.

#### driver.state(hd_id, cb)

  查询硬盘数据, cb => (err, info),
  info = { note, open_cnt -- 已经链接到硬盘的数量 }

#### driver.list(cb)

  列出所有硬盘的 id, cb => (err, hd_array)

#### driver.open_fs(hd_id, cb)

  在硬盘上打开文件系统api进行操作, 打开成功后 open_cnt+1; cb => (err, fs)

#### fs.quit(cb); dirver.close_fs(fs);

  fs 扩展, 关闭打开的 fs, open_cnt-1, 即使在 cb 中发生错误, fs 也无法使用.


# About

[nodejs fs api](https://nodejs.org/dist/latest-v0.12.x/docs/api/fs.html)
[cql docs](http://cassandra.apache.org/doc/latest/cql/index.html)
[JS Type](http://datastax.github.io/nodejs-driver/features/datatypes/)
