var tool = require('./tool.js');
var uuid = require('uuid');


module.exports = {
  create_driver   : create_driver,
  delete_driver   : delete_driver,
  list_drv        : list_drv,
  state_drv       : state_drv,
  gen_fs_cql      : gen_fs_cql,
};


const T_FILE = 1,
      T_DIR  = 2,
      T_LINK = 3,


function gen_fs_cql(hd_id, client_info) {
  if (!hd_id) throw new Error('hd_id not null');
  var safe_hd_id = tool.norms_name(hd_id);
  var nod = tool.node_table(safe_hd_id);
  var blk = tool.block_table(safe_hd_id);

  return {
    add_client_ref  : add_client_ref,
    sub_client_ref  : sub_client_ref,
    find_node       : find_node,
  };


  function add_client_ref() {
    return [
      'UPDATE driver_main SET open_client = open_client + ? where id = ?',
      [[client_info], hd_id]
    ];
  }


  function sub_client_ref() {
    return [
      'UPDATE driver_main SET open_client = open_client - ? where id = ?',
      [[client_info], hd_id]
    ];
  }


  function find_node(node_id) {
    return [
      'select * from' + nod + 'where id = ?',
      [node_id] ];
  }


  function create_dir(parent, name, mode) {
    var newid = uuid.v1();
    var child = {};
    child[name] = newid;

    return [
      'UPDATE ' + nod
        + ' SET name=?, mode=?, ctime=?, type=?'
        + ', parent=? Where id=?',
      [newid, name, mode, Date.now(), parent, T_DIR],
      
      'UPDATE ' + node + ' SET child = child + ?'
        + ' Where id=?',
      [child, parent],
    ]
  }
}


function create_driver(src_hd_id, desc) {
  if (!src_hd_id) throw new Error('src_hd_id not null');
  var hd_id   = tool.norms_name(src_hd_id);
  var nod     = tool.node_table(hd_id);
  var blk     = tool.block_table(hd_id);
  var rootid  = uuid.v1();
  var now     = Date.now();

  return [
  /* 0 */
    "insert into driver_main(id, create_tm, note, root) \
                      values(?,?,?,?)",

    [src_hd_id, now, desc, rootid],

  /* 2 */
    "insert into" + nod + "(id, name, ctime) values(?,?,?)",

    [rootid, '/', now],

  /* 4 */
    // 每个文件夹/文件/链接都有一个 node
    // name     -- 目录/文件名称
    // parent   -- 父级id
    // atime    -- 访问时间, mtime -- 修改时间, ctim -- 创建时间
    // mode     -- 访问描述符 777
    // uid      -- 所属用户, gid -- 所属组
    // blockid  -- 指向块
    // type     -- 1 文件, 2 目录, 3 链接
    "CREATE TABLE " + nod + "(\
        id        uuid PRIMARY KEY, \
        name      text,           \
        parent    uuid,           \
        atime     bigint,         \
        mtime     bigint,         \
        ctime     bigint,         \
        mode      smallint,       \
        uid       uuid,           \
        gid       uuid,           \
        blockid   int,            \
        type      tinyint,        \
        child     map<text, uuid> \
    );",

    // 每个文件一个 block
    // ref -- 被 node 引用的次数, 0 则应该销毁
    // blocksz -- 块大小, 字节
    // block   -- 存储块内容
    // tsize   -- 文件总大小, 字节
    "CREATE TABLE " + blk + "(\
        id        uuid PRIMARY KEY, \
        ref       int,        \
        tsize     bigint,     \
        blocksz   int,        \
        block     list<blob>, \
    )",

    "CREATE INDEX ON " + blk + "(ref)",
  ];
}


function delete_driver(hd_id) {
  if (!hd_id) throw new Error('hd_id not null');
  hd_id = tool.norms_name(hd_id);

  return [
    "DELETE from driver_main where id=?",
    "DROP TABLE " + tool.node_table(hd_id),
    "DROP TABLE " + tool.block_table(hd_id),
  ];
}


function list_drv() {
  return 'select id from driver_main';
}


function state_drv(hdid) {
  return ['select * from driver_main where id = ?', [hdid]];
}
