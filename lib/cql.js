var tool = require('./tool.js');
var uuid = require('uuid');
var plib = require('path').win32;


const T_FILE = 1,
      T_DIR  = 2,
      T_LINK = 3;


module.exports = {
  create_driver   : create_driver,
  delete_driver   : delete_driver,
  list_drv        : list_drv,
  state_drv       : state_drv,
  gen_fs_cql      : gen_fs_cql,
  T_FILE          : T_FILE,
  T_DIR           : T_DIR,
  T_LINK          : T_LINK,
};



function gen_fs_cql(hd_id, client_info) {
  if (!hd_id) throw new Error('hd_id not null');
  var safe_hd_id = tool.norms_name(hd_id);
  var nod = tool.node_table(safe_hd_id);
  var blk = tool.block_table(safe_hd_id);

  return {
    add_client_ref      : add_client_ref,
    sub_client_ref      : sub_client_ref,
    find_node           : find_node,
    create_dir          : create_dir,
    remove_dir          : remove_dir,
    find_node_with_path : find_node_with_path,
  };


  function add_client_ref() {
    return [
      "UPDATE dirver_ref SET ref = ref + 1 WHERE id = ?",
      [hd_id]
    ];
  }


  function sub_client_ref() {
    return [
      "UPDATE dirver_ref SET ref = ref - 1 WHERE id = ?",
      [hd_id]
    ];
  }


  function find_node(node_id) {
    return [
      'select * from' + nod + 'where id = ?',
      [node_id] ];
  }


  function find_node_with_path(_path) {
    return [
      'select * from' + nod + 'where complete = ?',
      [_path],
    ]
  }


  function create_dir(parent, name, mode, rpath) {
    var newid = uuid.v1();
    var child = {};
    child[name] = newid;

    return [
      'UPDATE ' + nod
        + ' SET name=?, mode=?, ctime=?, type=?'
        + ', parent=?, complete=? Where id=?',
      [name, mode, Date.now(), T_DIR, parent, rpath, newid],

      'UPDATE ' + nod + ' SET child = child + ?' + ' Where id=?',
      [child, parent],
    ]
  }


  function remove_dir(myself_node) {
    var child = [myself_node.name];
    return [
      'DELETE FROM ' + nod + 'Where id=?',
      [myself_node.id],

      'UPDATE ' + nod + ' SET child = child - ?' + ' Where id=?',
      [child, myself_node.parent],
    ];
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
    "insert into" + nod + "(id, name, ctime, type, complete) values(?,?,?,?,?)",
    [rootid, plib.sep, now, T_DIR, plib.sep],

  /* 4 */
    // 每个文件夹/文件/链接都有一个 node
    // name     -- 目录/文件名称
    // parent   -- 父级id, 根目录为 null
    // atime    -- 访问时间, mtime -- 修改时间, ctime -- 创建时间
    // mode     -- 访问描述符 777
    // uid,gid  -- 所属用户, 所属组
    // blockid  -- 指向块
    // type     -- 1 文件, 2 目录, 3 链接
    // child    -- 目录中的文件/文件夹/链接列表 <文件名, nod_id>, 没有为 null
    // complete -- 完整路径
    // link     -- 如果是 link 则链接到真实文件, 否则为 null
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
        blockid   uuid,           \
        type      tinyint,        \
        child     map<text, uuid>,\
        complete  text,           \
        link      uuid,           \
    );",

    "CREATE INDEX ON" + nod + "(complete)",

    // 每个文件一个 block
    // ref     -- 被 node 引用后记录这些 node
    // blocksz -- 块大小, 字节
    // block   -- 存储块内容
    // tsize   -- 文件总大小, 字节
    "CREATE TABLE " + blk + "(\
        id        uuid PRIMARY KEY, \
        ref_nod   list<uuid>, \
        tsize     bigint,     \
        blocksz   int,        \
        block     list<blob>, \
    )",
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
  return {
    main : ['select * from driver_main where id = ?', [hdid]],
    ref  : ['select * from driver_ref  where id = ?', [hdid]]
  };
}
