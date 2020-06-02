
# mysql-dbc-table

A Simple Toolkit for Human to CRUD Table of Mysql.
---

## Targets • 目标

- Simple Code but Flexible Usage.
- Restful CRUD Api to query Mysql table. 
- Fast predefine Column fields of table. (`$DbTable.fields()`)
- Auto formatted($fmt) sql data to js object.(`$_field_flag_hidden`)
- Support Soft Deletion and Soft Recovery. (`$_field_flag_hidden`)
 
```text
1. 代码简单，使用灵活
2. 接口易用，快速业务CURD
3. 快速预设表列信息 (`$DbTable.fields()`)
4. 自动格式转化sql查询结果为 js 常用对象 (`$fmt`)
5. 支持软删除, 软回收 (`$_field_flag_hidden`)
```

## Install • 安装
```shell
npm install mysql-dbc-table
```

## Depends • 依赖

- [sidorares/node-mysql2](https://github.com/sidorares/node-mysql2)
- [physacco/node-mysql-dbc](https://github.com/physacco/node-mysql-dbc)

## Samples • 示例

#### 1. Init Dbc: 初始化连接

```javascript
const mysql_dbc_table = require('./mysql-dbc-table');
const dbc_default = mysql_dbc_table.initDbc();
//; dbc_default.uri = "mysql://root:@localhost:3306/test"

//; customize dbc with {host,port,user,password,database,connectionLimit,queueLimit} 
const dbc_custom = mysql_dbc_table.initDbc({
    host: 'localhost',
    port: 3306,
    user: 'user',
    password: 'my_password',
    database: 'my_database',
});
// dbc_custom.uri = "mysql://${user}:${password}@${host}:${port}/${database}"

//; list tables of database
dbc_custom.showTablesAsync()
````

#### 2. execute sql:执行原始MySQL语句

```javascript
const sql_create_table = `
    CREATE TABLE \`user\` (
      \`id\` int(11) NOT NULL AUTO_INCREMENT,
      \`name\` varchar(64) NOT NULL,
      \`city\` varchar(64) NOT NULL,
      \`created_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
      \`modified_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      \`deleted\` boolean DEFAULT false,
      PRIMARY KEY (\`id\`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`;

dbc_custom.executeSqlAsync(sql_create_table);
```

#### 2. predefine Table and CRUD: 预设表列和CURD

```javascript
const DbTable = mysql_dbc_table.DbTable;

class User extends DbTable {
    static fields() {
        this._field_flag_hidden = "deleted";        
        return {
            id: {fmt: parseInt},
            name: {fmt: String, default: ''},
            city: {fmt: String, default: ''},
            created_time: {fmt: (...args) => new Date(...args)},
            modified_time: {fmt: (...args) => new Date(...args)},
            deleted : {fmt: Boolean, default: false},
        }
    }
}

const test_crud = async () => {
    // 0. init: 初始化
    // get instance of <DbTable:User> and check table exist
    const db_user = new User("user", dbc_custom);
    let is_existed = await db_user.existAsync();
    if (!is_existed) {
        // !require manually create table if not existed
        let res = await dbc_custom.executeSqlAsync(sql_create_table);
        is_existed = await db_user.existAsync();
    }
    

    // 1. create: 添加
    // insert one object
    const uid1 = await db_user.addAsync({name: 'usr1', city: 'A'});
    // insert many objects
    await db_user.addManyAsync([{name:'usr2', city: 'B'}, {name:'usr3', city: 'B'}]);
    

    // 2. read: 读取
    // real columns of mysql table 
    const columns = await db_user.showColumnsAsync()

    // following query results without deleted user if $_field_flag_hidden was set.
    // count rows
    const total = await db_user.countAsync();
    const count = await db_user.countAsync({city:'B'});
    // get one or raise 404
    const u0 = await db_user.getOr404Async({id:uid1});
    // get one or null
    const u1 = await db_user.getOrNullAsync({id:uid1});
    // support compare with predefined fields
    User.equal(u0, u1);          // ==> True
    // first or null
    const u2 = await db_user.findOneAsync({city: 'B'});
    // list with conditions
    const us1 = await db_user.findAsync({city: 'B'});
    // list all  
    const us2 = await db_user.findAsync();
    
    
    // 3. update : 修改
    // DbTable.updateAsync(conditions, updated_form) 
    db_user.updateAsync({id:uid}, {name:"Ein"})
    // soft deletion, it will hidden User<id:$uid> if redo previous query. 
    db_user.disableAsync({id:uid})
    // soft recovery,  User<id:$uid>
    db_user.enableAsync({id:uid})    
    

    // 4. delete : 删除 
    // hard deletion, never recovery by DbTable
    db_user.deleteAsync({id:uid})
    // !!! dangerous：delete all rows of table.
    db_user.deleteAsync()    // raise Exception to protect table
}
```
