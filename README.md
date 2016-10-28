# filesystem(fs) api 

fs api version on nodejs <=0.12.x

每个 cassandra 的 keyspace 可以保存多个 driver; 把 driver 想象成硬盘,  
每个硬盘都格式化成 ext4 格式, 可以在硬盘上打开文件系统(fs)进行操作.
应该给 fs 一个独立的 keyspace.

0.1.x 实现不支持用户权限/组权限, 不支持符号链接


# install

`npm install fs-cassandra --save`


# Usage

所有带有 Sync 结尾的同步式函数都不支持, 调用时会抛出异常.

```js
var cassandra = require('cassandra-driver');
var fs_cass = require('fs-cassandra');

var client = new cassandra.Client({ contactPoints: ['h1', 'h2'], keyspace: 'ks1'});
var driver = fs_cass.open_driver(client);

driver.delete('driver-name', callback);
driver.create('driver-name', callback);
driver.list(callback);

// 与 nodejs 的 fs 对象相同
var fs = driver.open_fs('driver-name', callback);

fs.quit();
driver.close_fs( fs_obj );
```

# Api

!!


# About

[nodejs fs api](https://nodejs.org/dist/latest-v0.12.x/docs/api/fs.html)

