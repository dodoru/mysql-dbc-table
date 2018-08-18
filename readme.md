
# mysql-dbc-table

creating and manipulating MySQL table by inheriting parent class<DbTable> with mysl-dbc

### Install

```shell
npm install mysql-dbc-table
```


### Usage Sample

#### Init mysql db connection

```javascript

const mysql_dbc_table = require('./mysql-dbc-table');

// default:  mysql://root:@localhost:3306/test
const dbc = mysql_dbc_table.initDbc();

// custom: mysql://${user}:${password}@${host}:${port}/${database}
const dbc1 = mysql_dbc_table.initDbc({
    host: 'localhost',
    port: 3306,
    user: 'developer',
    password: 'my_password',
    database: 'my_database',
});

```

#### Perform SQL

```javascript

const sql_create_table = `
    CREATE TABLE \`user\` (
      \`id\` int(11) NOT NULL AUTO_INCREMENT,
      \`name\` varchar(64) NOT NULL,
      \`created_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
      \`modified_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      \`deleted\` boolean DEFAULT false,
      PRIMARY KEY (\`id\`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`;

const step0 = await mysql_dbc_table.dbSqlSync(dbc, sql_create_table);

```

#### Use data model with DbTable
```
const DbTable = mysql_dbc_table.DbTable;
class User extends DbTable {
    static fields() {
        return {
            id: {fmt: parseInt},
            name: {fmt: String, default: ''},
            created_time: {fmt: (...args) => new Date(...args)},
            modified_time: {fmt: (...args) => new Date(...args)},
            deleted: {fmt: Boolean, default: false},
        }
    }
}

const db_user = new User('user', dbc);
const existed = await db_user.existSync();

const users = await db_user.findSync();
const user = await db_user.findOneSync();
```

##### 更多请参阅 test.js