# filesystem(fs) api 

fs api version on nodejs <=0.12.x


# install

`npm install fs-cassandra --save`


# Usage

每个 cassandra 的 keyspace 可以保存多个 driver, 把 deriver 想象成硬盘,  
每个硬盘都格式化成 ext4 格式, 可以在硬盘上打开文件系统进行操作.
所有带有 Sync 结尾的同步式函数都不支持, 调用时会抛出异常.

```js
var cassandra = require('cassandra-driver');
var fs_cass = require('fs-cassandra');

var client = new cassandra.Client({ contactPoints: ['h1', 'h2'], keyspace: 'ks1'});
var driver = fs_cass.open_driver(client);

driver.delete('driver-name', callback);
driver.create('driver-name', callback);

// 与 nodejs 的 fs 对象相同
var fs = driver.open_fs('driver-name', callback);

fs.quit();
driver.close_fs( fs_obj );
```

# Api