var tool = require('./tool.js');
var uuid = require('uuid');
var plib = require('path').win32;


const T_FILE      = 1,
      T_DIR       = 2,
      T_LINK      = 3;
      T_SYM_LINK  = 4;

const type_name = {
    1 : 'file',
    2 : 'directory',
    3 : 'hard link',
    4 : 'symbolic link',
};


module.exports = {
  create_driver   : create_driver,
  delete_driver   : delete_driver,
  list_drv        : list_drv,
  state_drv       : state_drv,
  gen_fs_cql      : gen_fs_cql,
  T_FILE          : T_FILE,
  T_DIR           : T_DIR,
  T_LINK          : T_LINK,
  T_SYM_LINK      : T_SYM_LINK,
  type_name       : type_name,
};



function gen_fs_cql(hd_id, client_info) {
  if (!hd_id) throw new Error('hd_id not null');
  var safe_hd_id = tool.norms_name(hd_id);
  var nod = tool.node_table(safe_hd_id);
  var blk = tool.block_table(safe_hd_id);
  var pth = tool.path_table(safe_hd_id);
  var block_size = 4096;

return {
  add_client_ref: function() {
    return [
      "UPDATE dirver_ref SET ref = ref + 1 WHERE id = ?",
      [hd_id]
    ];
  },


  sub_client_ref: function() {
    return [
      "UPDATE dirver_ref SET ref = ref - 1 WHERE id = ?",
      [hd_id]
    ];
  },


  find_node: function(node_id) {
    return [
      'select * from' + nod + 'where id = ?',
      [node_id]
    ];
  },


  find_path: function(pth_id) {
    return [
      'Select * From' + pth + 'where id = ?',
      [pth_id]
    ];
  },


  find_node_with_path: function(_path) {
    return [
      'select * from' + nod + 'where complete = ?',
      [_path],
    ]
  },


  create_node: function(p_parent, name, mode, rpath, _type) {
    var newid = uuid.v1();
    var tsize = null, blocksz = null;
    var child = {};
    child[name] = newid;

    if (_type == T_FILE) {
      tsize       = 0;
      blocksz     = block_size;
    }

    var ret = [
      'UPDATE ' + nod + ' SET ref_pth=ref_pth+?, tsize=?, blocksz=?, \
        mode=?, ctime=?, uid=?, gid=? Where id=?',
      [[newid], tsize, blocksz, mode, Date.now(), 0, 0, newid],

      'Update' + pth
        + 'Set nod_id=?, name=?, parent=?, type=?, complete=? \
        Where id=?',
      [newid, name, p_parent, _type, rpath, newid],

      'UPDATE' + pth + 'SET child = child + ?' + ' Where id=?',
      [child, p_parent],
    ];

    if (blocksz) {
      ret.block   = {
        id        : newid,
        blocksz   : blocksz,
        tsize     : 0,
      };
    }

    return ret;
  },


  unlink: function(path) {
    var child = [path.name];
    var ret = [
      'UPDATE ' + pth + ' SET child = child - ?' + ' Where id=?',
      [child, path.parent],

      'DELETE FROM ' + pth + 'Where id=?',
      [path.id],
    ];
    if (path.nod_id) {
      ret.push('Update' + nod + 'SET ref_pth = ref_pth - ? Where id=?');
      ret.push([[path.id], path.nod_id]);
    }
    return ret;
  },


  delete_nod: function(nod_id) {
    return {
      check: [
        'select ref_pth from' + nod + ' where id=?',
        [nod_id],
      ],
      delete: [
        'Delete From' + nod + 'Where id=?',
        [nod_id],
        'Delete From' + blk + 'Where id=?',
        [nod_id],
      ],
    };
  },


  update_time: function(fid, atime, mtime) {
    return [
      'UPDATE ' + nod + 'SET atime=?, mtime=? WHERE id=?',
      [atime, mtime, fid]
    ];
  },


  change_owner: function(fid, uid, gid) {
    return [
      'UPDATE' + nod + 'SET gid=?, uid=? WHERE id=?',
      [gid, uid, fid]
    ];
  },


  change_mode: function(nid, mode) {
    return [
      'UPDATE' + nod + 'SET mode=? WHERE id=?',
      [mode, nid]
    ];
  },


  write_block: function(bid, idx, buf) {
    return [
      'Update' + blk + 'SET block=? Where id=? and page=?',
      [buf, bid, idx]
    ];
  },


  read_block: function(bid, idx) {
    return [
      'Select block from' + blk + 'Where id=? and page=?',
      [bid, idx],
    ];
  },


  update_size: function(id, tsize) {
    return [
      'Update' + nod + 'Set tsize=? Where id=?',
      [tsize, id]
    ];
  },

}; // return end.
}


function create_driver(src_hd_id, desc) {
  if (!src_hd_id) throw new Error('src_hd_id not null');
  var hd_id   = tool.norms_name(src_hd_id);
  var nod     = tool.node_table(hd_id);
  var blk     = tool.block_table(hd_id);
  var pth     = tool.path_table(hd_id);
  var rootid  = uuid.v1();
  var now     = Date.now();

  return [
  /* 0 */
    "insert into driver_main(id, create_tm, note, root) \
                      values(?,?,?,?)",
    [src_hd_id, now, desc, rootid],

  /* 2 */
    "insert into" + pth + "(id, name, type, complete, nod_id) values(?,?,?,?,?)",
    [rootid, plib.sep, T_DIR, plib.sep, rootid],

  /* 4 */
    "Insert Into" + nod + "(id, btime, mode, ref_pth) values(?,?,?,?)",
    [rootid, now, 438, [rootid]],

  /* 6 */
    // 每个文件夹/文件/链接都有一个 node,
    //          1234 在各种类型下属性的有效性, '-' 总是有效, 'x' 无效时为null,
    // atime    ---- 访问时间,
    // mtime    ---- 修改时间,
    // btime    ---- 创建时间,
    // ctime    ---- 改变时间
    // mode     ---- 访问描述符 777
    // uid,gid  ---- 所属用户, 所属组
    // ref_pth  ---- 被 pth 引用的 pthid
    // tsize    -x-x 文件总大小, 字节
    // blocksz  -x-x 块大小, 字节
    "CREATE TABLE " + nod + "(      \
        id        uuid PRIMARY KEY, \
        atime     bigint,           \
        mtime     bigint,           \
        ctime     bigint,           \
        btime     bigint,           \
        mode      smallint,         \
        uid       int,              \
        gid       int,              \
        ref_pth   list<uuid>,       \
        tsize     int,              \
        blocksz   smallint,         \
    );",

    // 文件关系图
    // name     ---- 目录/文件名称
    // nod_id   ---x 指向 nod 的指针
    // parent   ---- 父级 pth_id, 根目录为 null
    // child    x-xx 目录中的文件/文件夹/链接列表 <文件名, pth_id>,
    // type     ---- 1 文件, 2 目录, 3 硬链接, 4 软链接
    // sym_link xxx- 软链接指向的路径, 可以是相对路径
    // complete ---- 完整路径
    "CREATE TABLE" + pth + '(\
        id        uuid PRIMARY KEY, \
        nod_id    uuid,             \
        name      text,             \
        parent    uuid,             \
        child     map<text, uuid>,  \
        type      tinyint,          \
        sym_link  text,             \
        complete  text,             \
    )',

    "CREATE INDEX ON" + pth + "(complete)",

    // 每个文件一个 block
    // id      -- 总是与 nod.id 相等
    // page    -- 分页, 页大小在 nod.blocksz 中定义
    // block   -- 存储块内容
    "CREATE TABLE " + blk + "(      \
        id        uuid,             \
        page      int,              \
        block     blob,             \
        PRIMARY KEY(id, page)       \
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
    "DROP TABLE " + tool.path_table(hd_id),
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
