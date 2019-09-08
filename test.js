const assert = require('assert');
const mysql_dbc_table = require('./index');
const db_util = require('./lib/db_util');

const DbTable = mysql_dbc_table.DbTable;
const log = console.log.bind(console);
const dbc = mysql_dbc_table.initDbc();
log('[mysqlURI]', dbc.uri);

class User extends DbTable {
    static fields() {
        return {
            id: {fmt: parseInt},
            name: {fmt: db_util.trimString, default: ''},
            note: {fmt: db_util.varStr, default: null},
            created_time: {fmt: (...args) => new Date(...args)},
            modified_time: {fmt: (...args) => new Date(...args)},
            deleted: {fmt: Boolean, default: false},
        }
    }
}

const sql_create_table = `
    CREATE TABLE \`user\` (
      \`id\` int(11) NOT NULL AUTO_INCREMENT,
      \`name\` varchar(64) NOT NULL,
      \`note\` varchar(32) default NULL,
      \`created_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
      \`modified_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      \`deleted\` boolean DEFAULT false,
      PRIMARY KEY (\`id\`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`;


// testDropTable().then().catch();
const testEnsureExist = async (db_model) => {
    log('toString', String(db_model));
    log('toJSON', JSON.stringify(db_model));
    log('showTables', await dbc.showTablesAsync());

    const existed = await db_model.existAsync();
    if (!existed) {
        const s0 = await mysql_dbc_table.dbSqlAsync(dbc, sql_create_table);
        assert.equal(s0[0].serverStatus, 2);
        log(`create table<${db_model.name}>`, s0);
        const s1 = await db_model.existAsync();
        assert.equal(s1, true);
    }
}

const testDrop = async () => {
    const sql_drop_table = `Drop Table \`user\`;`
    const state = await mysql_dbc_table.dbSqlAsync(dbc, sql_drop_table)
    log('drop!!!', sql_drop_table, state);
}


const testMain = async () => {
    // mysql://root:@localhost:3306/test
    // init table
    const db_user = new User('user', dbc);
    // log('info', db_user);
    await testEnsureExist(db_user);
    log(DbTable.name, User.name);
    log('showColumns', await db_user.showColumnsAsync())
    // test model
    const ts = new Date().getTime();
    const u = {name: `tester_${ts}`};

    const s2 = await db_user.findOneAsync(u);
    assert.equal(s2, null);

    const user_id = await db_user.addAsync(u);
    console.log('add user, id:', user_id);

    const s4 = await db_user.findOneAsync(u);
    console.log('find user', s4);
    assert.equal(u.name, s4.name);

    const s5 = await db_user.findOneAsync({id: s4.id});
    assert.ok(User.equal(s4, s5));

    const s6 = await db_user.findAsync();
    const s7 = await db_user.countAsync();
    assert.equal(s6.length, s7);

    const s8 = await db_user.updateAsync(u, {'deleted': true});
    const s9 = await db_user.findOneAsync(u);
    const s10 = await db_user.findOneAsync(u, false);
    console.log('count of updated rows:', s8);
    assert.equal(s8, 1);
    assert.equal(s9, null);
    assert.equal(s10.deleted, true);

    // test update
    mysql_dbc_table.db_table.SqlConfig.allow_multiple_on_find_one = true
    const ur = await db_user.findOneAsync({deleted: true}, false);
    console.log('findOne', ur);
    const ur1 = await db_user.updateAsync({id: ur.id}, {name: ur.name.replace('tester_', 'dev_')});
    console.log('update=>', ur1);
    const ur2 = await db_user.findOneAsync({id: ur.id});
    console.log('result :', ur2);

    mysql_dbc_table.db_table.SqlConfig.allow_multiple_on_find_one = false

    // test upsert
    const ur3 = await db_user.findOneAsync({id: 1}, false);
    console.log('user<id:1>', ur3);
    const {op, data, state} = await db_user.upsertAsync({id: 1}, {name: 'admin', deleted: false});
    console.log(`${op} [${state}] user<id:1> as admin`, data);
    const ur5 = await db_user.findOneAsync({id: 1});
    console.log('user<id:1>', ur5);

    const qry = {
        opts: {
            ge: {id: 2,},
            le: {id: 6,},
            eq: {deleted: true},
            in: {id: [1, 2, 4, 5, 7]},
            like: {name: 'tester%'}
        },
        order: {
            key: 'id',
            desc: true,
        }
    };
    const ds = await db_user.queryAsync(qry);
    console.log(222, ds);

    if (ds.length > 0) {
        const d3 = await db_user.upsertManyAsync(ds);
        console.log(333, d3)

        const d4 = await db_user.replaceManyAsync(ds);
        console.log(444, d4);
    }


    const d5 = await db_user.replaceOneAsync({name: "admin"});
    console.log(555, d5);
    let d5_id = d5.insertId;
    const d6 = await db_user.replaceOneAsync({name: `admin_${d5_id}`, id: d5_id});
    console.log(666, d6);

    const d7 = await db_user.disableAsync({id: d5_id});
    console.log(777, d7)

    const d8 = await db_user.enableAsync({id: d5_id});
    console.log(888, d8)

    const d9 = await db_user.deleteAsync({id: d5_id});
    console.log(999, d9)

    // eq null
    const d10 = await db_user.queryAsync({opts: {eq: {note: null}}})
    console.log(1000, d10.length, d10[0])
    assert.equal(d10.length, 0)

    // is null
    const d11 = await db_user.queryAsync({opts: {is: {note: null}}})
    console.log(1004, d11.length)

    const d12 = await db_user.findAsync({note: null}, false)
    console.log(1005, d12.length)
    assert.equal(d12.length, d11.length)

    // update config
    mysql_dbc_table.db_table.SqlConfig.enable_undefined = true;
    const d13 = await db_user.findOneAsync({note: null}, false, "*", null, true)
    console.log(1006, d13)

    const d14 = await db_user.findOneAsync({note: undefined}, false, res = "id")
    console.log(1007, d14)

    const d15 = await db_user.findAsync();
    console.log(1008, d15.length, d15)

    if (d15.length > 0) {
        const d16 = await db_user.updateAsync({id: d15[0].id}, {note: 'test undefined'});
        console.log(1010, d16)
    }

    try {
        const d17 = await db_user.findOneAsync({note: undefined}, false)
        console.log(1011, d17)
    } catch (e) {
        assert(e.errno === errno)
    }
};

const testUtil = () => {
    let k = db_util.varStr(undefined);
    let k1 = db_util.varStr('undefine');
    let k2 = db_util.varStr('undefined');
    let k3 = db_util.varStr('None');
    let k4 = db_util.varStr('NULL');
    console.log(k, k1, k2, k3, k4)
}


// testDrop().then().catch();
testMain().then().catch();
testUtil();