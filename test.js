const assert = require('assert');
const mysql_dbc_table = require('./index');

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


const testMain = async () => {
    const log = console.log.bind(console);
    const dbc = mysql_dbc_table.initDbc();
    log('[mysqlURI]', dbc.uri);
    // mysql://root:@localhost:3306/test

    // init table
    const db_user = new User('user', dbc);
    const existed = await db_user.existSync();
    if (!existed) {
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
        const s0 = await mysql_dbc_table.dbSqlSync(dbc, sql_create_table);
        assert.equal(s0[0].serverStatus, 2);
        console.log('create table<user>', s0);

        const s1 = await db_user.existSync();
        assert.equal(s1, true);
    }

    // test model
    const ts = new Date().getTime();
    const u = {name: `tester_${ts}`};

    const s2 = await db_user.findOneSync(u);
    assert.equal(s2, null);

    const user_id = await db_user.addSync(u);
    console.log('add user, id:', user_id);

    const s4 = await db_user.findOneSync(u);
    console.log('find user', s4);
    assert.equal(u.name, s4.name);

    const s5 = await db_user.findOneSync({id: s4.id});
    assert.ok(User.equal(s4, s5));

    const s6 = await db_user.findSync();
    const s7 = await db_user.countSync();
    assert.equal(s6.length, s7);

    const s8 = await db_user.updateSync(u, {'deleted': true});
    const s9 = await db_user.findOneSync(u);
    const s10 = await db_user.findOneSync(u, false);
    console.log('count of updated rows:', s8);
    assert.equal(s8, 1);
    assert.equal(s9, null);
    assert.equal(s10.deleted, true);

    if (s7 >= 2) {
        const cnt = parseInt(s7 / 2);
        const s11 = await db_user.findLimitSync(cnt);
        assert.equal(s11.length, cnt);
    }

    // test update
    const ur = await db_user.findOneSync();
    console.log('findOne', ur);
    const ur1 = await db_user.updateSync({id: ur.id}, {name: ur.name.replace('tester_', 'dev_')});
    console.log('update=>', ur1);
    const ur2 = await db_user.findOneSync({id: ur.id});
    console.log('result :', ur2);

    // test upsert
    const ur3 = await db_user.findOneSync({id: 1}, false);
    console.log('user<id:1>', ur3);
    const {op, data, state} = await db_user.upsertSync({id: 1}, {name: 'admin', deleted: false});
    console.log(`${op} [${state}] user<id:1> as admin`, data);
    const ur5 = await db_user.findOneSync({id: 1});
    console.log('user<id:1>', ur5);

    const qry = {
        opts: {
            ge: {id: 2,},
            le: {id: 6,},
            eq: {deleted: false},
            in: {id: [1, 2, 4, 5, 7]},
            like: {name: 'tester%'}
        },
        order: {
            key: 'id',
            desc: true,
        }
    };
    const ds = await db_user.querySync(qry);
    console.log(222, ds);

};


testMain().then().catch();