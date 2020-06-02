const assert = require('assert');
const mysql_dbc_table = require('./index');

const DbTable = mysql_dbc_table.DbTable;

class User extends DbTable {
    static fields() {
        this._field_primary_key = "id";
        this._field_flag_hidden = "deleted";
        return {
            id: {fmt: parseInt},
            name: {fmt: String, default: ''},
            city: {fmt: String, default: ''},
            created_time: {fmt: (...args) => new Date(...args)},
            modified_time: {fmt: (...args) => new Date(...args)},
            deleted: {fmt: Boolean, default: false},
        }
    }
}


const t_tablename = "test_db_table_user";

// if you don't want to console output the debug message, just override the function:`log()`.
const log = console.log.bind(console);


const testMain = async () => {
    const dbc = mysql_dbc_table.initDbc();
    log('[mysqlURI]', dbc.uri);
    // mysql://root:@localhost:3306/test
    // init table
    const db_user = new User(t_tablename, dbc);
    log(DbTable.name, User.name, db_user._field_flag_hidden, db_user._field_primary_key);
    log('info', db_user);
    log('toString', String(db_user));
    log('toJSON', JSON.stringify(db_user));
    log('showTables', await dbc.showTablesAsync());

    const existed = await db_user.existAsync();
    if (!existed) {
        const sql_create_table = `
            CREATE TABLE \`${t_tablename}\` (
              \`id\` int(11) NOT NULL AUTO_INCREMENT,
              \`name\` varchar(64) NOT NULL default '',
              \`city\` varchar(64) NOT NULL default '',
              \`created_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
              \`modified_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              \`deleted\` boolean DEFAULT false,
              PRIMARY KEY (\`id\`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;
        const s0 = await mysql_dbc_table.dbSqlAsync(dbc, sql_create_table);
        assert.equal(s0[0].serverStatus, 2);
        log('create table<user>', s0);

        const s1 = await db_user.existAsync();
        assert.equal(s1, true);
    }

    log('showColumns', await db_user.showColumnsAsync())

    // test model
    const ts = new Date().getTime();
    const u = {name: `tester_${ts}`};

    const s2 = await db_user.findOneAsync(u);
    assert.equal(s2, null);

    const user_id = await db_user.addAsync(u);
    log('add user, id:', user_id);

    const xxxx = await db_user.addManyAsync([{name: `tester_${ts}_1`}, {name: `tester_${ts}_2`}, {name: `tester_${ts}_3`}]);
    log('add many, xxxx:', xxxx);

    const s4 = await db_user.findOneAsync(u, false);
    log('find user', s4);
    assert.equal(u.name, s4.name);

    const s5 = await db_user.findOneAsync({id: s4.id});
    assert.ok(User.equal(s4, s5));

    const s6 = await db_user.findAsync();
    const s7 = await db_user.countAsync();
    assert.equal(s6.length, s7);

    const s8 = await db_user.updateAsync(u, {'deleted': true});
    const s9 = await db_user.findOneAsync(u);
    const s10 = await db_user.findOneAsync(u, false);
    log('count of updated rows:', s8);
    assert.equal(s8, 1);
    assert.equal(s9, null);
    assert.equal(s10.deleted, true);

    // test update
    const ur = await db_user.findOneAsync({deleted: true}, false);
    log('findOne', ur);
    const ur1 = await db_user.updateAsync({id: ur.id}, {name: ur.name.replace('tester_', 'dev_')});
    log('update=>', ur1);
    const ur2 = await db_user.findOneAsync({id: ur.id});
    log('result :', ur2);

    // test upsert
    const ur3 = await db_user.findOneAsync({id: 1}, false);
    log('user<id:1>', ur3);
    const {op, data, state} = await db_user.upsertAsync({id: 1}, {name: 'admin', deleted: false});
    log(`${op} [${state}] user<id:1> as admin`, data);
    const ur5 = await db_user.findOneAsync({id: 1});
    log('user<id:1>', ur5);

    const qry = {
        opts: {
            ge: {id: 1,},
            le: {id: 6,},
            eq: {city: "A"},
            in: {id: [1, 2, 4, 5, 7]},
            like: {name: 'tester%'}
        },
        order: {
            key: 'id',
            desc: true,
        }
    };
    const ds = await db_user.queryAsync(qry);
    log(222, ds);

    if (ds.length > 0) {
        const d3 = await db_user.upsertManyAsync(ds);
        log(333, d3)

        const d4 = await db_user.replaceManyAsync(ds);
        log(444, d4);
    }

    const d5 = await db_user.replaceOneAsync({name: "admin"});
    log(555, d5);
    let d5_id = d5.insertId;

    const d6 = await db_user.replaceOneAsync({name: `admin_${d5_id}`, id: d5_id});
    log(666, d6);

    const d7 = await db_user.disableAsync({id: d5_id});
    log(777, d7)

    const d8 = await db_user.enableAsync({id: d5_id});
    log(888, d8)

    const d9 = await db_user.deleteAsync({id: d5_id});
    log(999, d9)

    try {
        const d10 = await db_user.deleteAsync();
        log(`Failed! you have deleted all rows of ${db_user._cls}`)
    } catch (e) {
        log(e)
        log(`Success ! you have protect rows of ${db_user._cls}`)
    }
};


testMain().then().catch();