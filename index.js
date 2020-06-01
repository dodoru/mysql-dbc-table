/*
*  Mysql Dbc Table:
*    A Simple Toolkit for Human to CRUD Table of Mysql.
*  copyright@https://github.com/dodoru/mysql-dbc-table
* */

const mysql_dbc = require('mysql-dbc');

const initDbc = (config = {}) => {
    const cfg = {
        // required
        host: config.host || 'localhost',
        port: config.port || 3306,
        user: config.user || 'root',
        password: config.password || '',
        database: config.database || 'test',
        // optional
        connectionLimit: config.connectionLimit || 20,
        queueLimit: config.queueLimit || 10,
    };

    const dbc = mysql_dbc.createDbc();
    dbc._cls = `<MysqlDbc:${cfg.database}>`;
    dbc.init(cfg);
    dbc.config = cfg;
    dbc.uri = `mysql://${cfg.user}:${cfg.password}@${cfg.host}:${cfg.port}/${cfg.database}`;

    dbc.showTablesAsync = async () => {
        const sql = `show tables from ${cfg.database}`;
        const func = dbc.withConnection(
            function () {
                return this.conn.query(sql)
            });
        const result = await func();
        const [rows, columns] = result;
        const name = columns[0].name;
        return rows.map(m => m[name]);
    };

    dbc.executeSqlAsync = async (sql, args) => {
        const func = dbc.withConnection(
            function () {
                return this.conn.query(sql, args)
            });
        return await func();
    };

    return dbc;
};

// singleton dbc pool
const dbcPool = {
    dbcfgs: {},
    _pools: {},
    setDbc: (name, config) => {
        const cfg = dbcPool.dbcfgs[name];
        const uri = `mysql://${config.host}:${config.port}/${config.database}`;
        if (cfg) {
            console.log(`[dbcPool] interrupt connecting to mysql://${cfg.host}:${cfg.port}/${cfg.database}`);
            console.log(`[dbcPool] reconnect to ${uri}`);
        } else {
            console.log(`[dbcPool] connect to ${uri}`);
        }
        dbcPool.dbcfgs[name] = config;
    },
    getDbc: (name) => {
        let dbc = dbcPool._pools[name];
        if (!dbc) {
            const cfg = dbcPool.dbcfgs[name];
            if (cfg) {
                dbc = initDbc(cfg);
                dbcPool._pools[name] = dbc;
            } else {
                throw new Error(`[dbcPool] no found dbc<${name}>`)
            }
        }
        return dbc;
    },
    getPool: () => {
        return dbcPool._pools;
    },
};


const K_OP = {
    eq: '=',
    ne: '!=',
    gt: '>',
    ge: '>=',
    lt: '<',
    le: '<=',
    in: 'IN',
    like: 'LIKE',
    not_in: 'NOT IN',
};


// todo : rewrite class<DbQuery>
const sqlFormat = (opts = {}, order = {}, limit) => {
    const qs = [];
    const args = [];
    for (let mark in opts) {
        const op = K_OP[mark];
        if (op === undefined) {
            throw new Error(`invalid mysql.OP<${mark}>`)
        }
        const form = opts[mark];
        let fs = Reflect.ownKeys(form);
        let vs = fs.map(key => form[key]);
        let q;
        if ((op === K_OP.in || op === K_OP.not_in) && vs instanceof Array) {
            q = fs.map(key => ` ${key} ${op} (?) `).join(' AND ');
        } else {
            q = fs.map(key => ` ${key} ${op} ? `).join(' AND ');
        }
        qs.push(q);
        args.push(...vs);
    }
    let query = qs.join(' AND ');
    if (query) {
        query = ` WHERE ${query} `;
    }
    // 排序
    if (order && order instanceof Object) {
        // default: 'ASC' : 升序
        // The DESC keyword is used to sort the query result set in a descending order.
        let key = order.key;
        if (key) {
            if (key instanceof Array) {
                key = key.join(',')
            }
            query += ` ORDER BY ${key} `;
            if (order.desc) {
                query += ' DESC '
            }
        }
    }
    if (limit instanceof Array && limit.length <= 2) {
        limit = limit.join(',');
    }
    if (limit) {
        query += ` LIMIT ${limit} `;
    }
    return {query, args};
};


const dbSqlAsync = async (dbc, sql, args) => {
    /* return:
        $select => {rows, columns}
    */
    const func = dbc.withConnection(
        function () {
            return this.conn.query(sql, args)
        });
    return await func();
};


class DbTable {
    constructor(tablename_or_dbc, dbc) {
        // $dbc is required, if undefined try to init from $1:tablename_or_dbc
        dbc = dbc || tablename_or_dbc;
        this._cls = `<DbTable:${this.constructor.name}>`;
        if (typeof (tablename_or_dbc) === "string") {
            this.tablename = tablename_or_dbc;
        } else {
            this.tablename = this.constructor.name;
        }
        if (dbc instanceof Object && String(dbc.uri).startsWith("mysql://")) {
            this.dbc = dbc;
        } else {
            throw new Error(`${this._cls}: init [${this.tablename}] with invalid dbc ...`)
        }
    }

    info() {
        const model = this.constructor.name;
        const tablename = this.tablename;
        const {host, port, user, database} = this.dbc.config;
        return {model, tablename, database, host, port, user}
    }

    /* support String() */
    toString() {
        const {model, tablename, database, host, port, user} = this.info();
        return `[DbTable:${model}:${tablename}] dbc=${user}@${host}:${port}/${database}`;
    }

    /* support JSON.stringify() */
    toJSON() {
        return this.toString();
    }

    /*
    * <usage>: define column fields of DbTable, require override in SubClass of DbTable
    * options:
          `fmt` : <Function> : convert raw data from `node-mysql2` to javascript object.
          `default` : <value>: optional to init a row object to insert into mysql table.
    * return: {<$column>: {fmt: <$function>}}
    * */
    static fields() {
        return {
            id: {
                fmt: parseInt,
                default: 0,
            },
            deleted: {
                fmt: Boolean,
                default: false,
            },
        }
    }

    /*
    * <usage>: show column fields of Mysql Table
    * sample item of <List:$cols> and <List:$rows> from results of sql query.
     col = {
            catalog: 'def',
            schema: 'information_schema',
            name: 'Field',
            orgName: 'COLUMN_NAME',
            table: 'COLUMNS',
            orgTable: 'COLUMNS',
            characterSet: 224,
            columnLength: 256,
            columnType: 253,
            flags: 1,
            decimals: 0
     }
     row = TextRow {
            Field: 'id',
            Type: 'int(11)',
            Null: 'NO',
            Key: 'PRI',
            Default: null,
            Extra: 'auto_increment'
     }
    * return: <List: [ <Object:{Field, Type, Key, Default, Extra}> ] >
    * */
    async showColumnsAsync() {
        const dbc = this.dbc;
        const tablename = this.tablename;
        const sql = `show columns from ${tablename}`;
        const result = await dbSqlAsync(dbc, sql);
        const [rows, cols] = result;
        return rows;
    }

    /*
    * return: <List: [ <String:$ColumnName> ]>
    * */
    async listColumnNamesAsync() {
        const columns = await this.showColumnsAsync();
        return columns.map(m => m.Field);
    }

    async ensureColumnsAsync(...column_names) {
        const fields = await this.listColumnNamesAsync();
        const fields_set = new Set(fields);
        for (let column_name of column_names) {
            const is_existed = fields_set.has(column_name);
            if (!is_existed) {
                throw new Error(`DbTable<${this._cls}> unknown field "${column_name}"`);
            }
        }
    }

    static toData(row) {
        const fields = this.fields();
        const data = {};
        for (let key in fields) {
            const field = fields[key];
            let value;
            if (key in row) {
                value = field.fmt(row[key]);
            } else {
                value = field.default;
            }
            if (value !== undefined) {
                data[key] = value;
            }
        }
        return data;
    }

    static format(rows) {
        const self = this;
        return rows.map(m => self.toData(m));
    }

    static strictForm(object) {
        const fields = this.fields();
        const form = {};
        for (let key in fields) {
            const fmt = fields[key].fmt;
            const value = object[key];
            if (value !== undefined && fmt instanceof Function) {
                form[key] = fmt(value);
            }
        }
        return form;
    }

    static queryForm(filter, ensureNotDeleted) {
        const form = Object.assign({}, filter);
        const fields = this.fields();
        if ('deleted' in fields) {
            if (ensureNotDeleted === undefined || ensureNotDeleted) {
                form.deleted = false;
            }
        }
        return form;
    }

    static equal(a, b) {
        const fields = this.fields();
        for (let key in fields) {
            const field = fields[key];
            let v1, v2;
            if (key in a) {
                v1 = field.fmt(a[key]);
            } else {
                v1 = field.default;
            }
            if (key in b) {
                v2 = field.fmt(a[key]);
            } else {
                v2 = field.default;
            }
            if (v1 instanceof Date && v2 instanceof Date) {
                if (v1.getTime() !== v2.getTime()) {
                    return false;
                }
            } else {
                if (v1 !== v2) {
                    return false;
                }
            }
        }
        return true;
    }

    async sqlAsync(sql, args) {
        const {dbc, tablename} = this;
        return await dbSqlAsync(dbc, sql, args);
    }

    async selectAsync(sql, args) {
        const {dbc, tablename} = this;
        const func = dbc.withConnection(
            function () {
                return this.doSelect(sql, args)
            }
        );
        return await func();
    }

    async existAsync() {
        const sql = `
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ? 
        `;
        const tbls = await this.selectAsync(sql, [this.dbc.config.database, this.tablename]);
        // expect length === 1;
        return tbls.length > 0;
    }

    async queryAsync(qry = {}) {
        const opts = qry.opts || {};
        const order = qry.order || {};
        const limit = qry.limit;
        let res = qry.res || '*';
        if (res === "*") {
            let fds = this.constructor.fields();
            res = Object.keys(fds).join(",");
        }
        const {dbc, tablename} = this;
        const {query, args} = sqlFormat(opts, order, limit);
        const sql = `SELECT ${res} FROM ${tablename} ${query}; `;
        const func = dbc.withConnection(
            function () {
                return this.doSelect(sql, args)
            }
        );
        return await func();
    }

    async countAsync(filter = {}, ensureNotDeleted) {
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(filter, ensureNotDeleted);
        const {query, args} = sqlFormat({eq: form});
        const sql = `SELECT count(*) as count FROM ${tablename} ${query};`;
        const result = await dbSqlAsync(dbc, sql, args);
        const [rows, columns] = result;
        return rows[0].count;
    }


    async findAsync(filter = {}, ensureNotDeleted, res = '*', order = {}, limit) {
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(filter, ensureNotDeleted);
        const {query, args} = sqlFormat({eq: form}, order, limit);
        const sql = `SELECT ${res} FROM ${tablename} ${query};`;
        const result = await dbSqlAsync(dbc, sql, args);
        const [rows, fields] = result;
        return rows;
    }

    async findOneAsync(filter = {}, ensureNotDeleted, res = '*', order = {}) {
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(filter, ensureNotDeleted);
        const {query, args} = sqlFormat({eq: form}, order, 1);
        const sql = `SELECT ${res} FROM ${tablename} ${query};`;
        const result = await dbSqlAsync(dbc, sql, args);
        const [rows, fields] = result;
        return rows[0] || null;
    }

    async getOr404Async(filter = {}, ensureNotDeleted, res = '*') {
        const obj = await this.findOneAsync(filter, ensureNotDeleted, res);
        if (obj === null) {
            const e = new Error();
            e.message = `Not Found <${this.tablename}:${JSON.stringify(filter)}>`;
            e.status = 404;
            e.errno = 40400;
            e.sqlState = 'DataNotFound';
            e.sqlMessage = e.message;
            throw e;
        }
        return obj;
    }

    async findLimitAsync(limit = 1, filter = {}, order = {}, ensureNotDeleted) {
        // size: int : 个数
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(filter, ensureNotDeleted);
        const {query, args} = sqlFormat({eq: form}, order, limit);
        const sql = `SELECT * from ${tablename} ${query} ;`;
        return await this.selectAsync(sql, args);
    }

    async findOneByFieldsAsync(field_keys, field_values) {
        const {dbc, tablename} = this;
        const func = dbc.withConnection(
            function () {
                return this.selectOneByFields(tablename, field_keys, field_values)
            }
        );
        return await func();
    }

    async addAsync(object) {
        const {dbc, tablename} = this;
        const func = dbc.withConnection(
            function () {
                return this.insertOneObject(tablename, object);
            }
        );
        return await func();
    }

    async addManyAsync(objects) {
        const {dbc, tablename} = this;
        const func = dbc.withConnection(
            function () {
                return this.insertManyObjects(tablename, objects);
            }
        );
        return await func();
    }

    async updateAsync(filter = {}, updated_form = {}, ensureNotDeleted) {
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(filter, ensureNotDeleted);
        const filter_fields = Reflect.ownKeys(form);
        const filter_values = filter_fields.map(key => form[key]);

        const updated_fields = Reflect.ownKeys(updated_form);
        const updated_values = updated_fields.map(key => updated_form[key]);
        if (updated_fields.length === 0) {
            return 0
        } else {
            const fws = filter_fields.map(k => `${k} = ?`).join(' AND ');
            const ups = updated_fields.map(k => `${k} = ?`).join(', ');
            const sql = `UPDATE ${tablename} SET ${ups} WHERE ${fws}`;
            const func = dbc.withConnection(
                function () {
                    return this.doUpdate(sql, [...updated_values, ...filter_values])
                }
            );
            return await func();
        }
    }

    async ensureAsync(object) {
        const item = await this.findOneAsync(object, false);
        if (item === null) {
            return await this.addAsync(object);
        } else {
            if (item.deleted) {
                return await this.enableAsync(item);
            }
        }
    }

    async upsertAsync(filter = {}, updated_form = {}) {
        const item = await this.findOneAsync(filter, false);
        let op, state, data;
        if (item === null) {
            op = 'insert';
            data = Object.assign(filter, updated_form);
            state = await this.addAsync(data);
        } else {
            if (item.deleted) {
                updated_form.deleted = false;
            }
            op = 'update';
            data = Object.assign({}, item, updated_form);
            state = await this.updateAsync(filter, updated_form, false);
        }
        return {op, data, state}
    }

    async replaceOneAsync(object) {
        /*
        * mysql insert replace require privileges of delete and insert .
        * https://dev.mysql.com/doc/refman/8.0/en/replace.html

        @return : ResultSetHeader {
            fieldCount: 0,
            affectedRows: 1,
            insertId: 38,
            info: '',
            serverStatus: 2,
            warningStatus: 0
            }
        */

        const fields = Reflect.ownKeys(object);
        const values = fields.map(key => object[key]);
        const {dbc, tablename} = this;
        const fs = `(${fields.join(', ')})`;
        const vs = `(${fields.map(m => "?").join(', ')})`;
        // const lines = new Array(count).fill(vs);
        const sql = `REPLACE INTO ${tablename} ${fs} VALUES ${vs};`;
        const [result, field] = await dbSqlAsync(dbc, sql, values);
        result.op = 'replace_into';
        return result;
    }

    async replaceManyAsync(objects, strict = true) {
        /*
        @strict: strictForm
        @return: ResultSetHeader {
            fieldCount: 0,
            affectedRows: 2,
            insertId: 4,
            info: 'Records: 2  Duplicates: 0  Warnings: 0',
            serverStatus: 2,
            warningStatus: 0,
            }
        */
        if (objects instanceof Array) {
            const count = objects.length;
            if (count === 0) {
                throw new Error(`DbTable<${this._cls}>: no objects to update`)
            }

            if (strict) {
                objects = objects.map(obj => this.constructor.strictForm(obj))
            }

            const {dbc, tablename} = this;
            const fields = Reflect.ownKeys(objects[0]);
            const values = objects.map(obj => fields.map(key => obj[key]));

            const fs = `(${fields.join(', ')})`;
            const vs = `(${fields.map(m => "?").join(', ')})`;
            const lines = new Array(count).fill(vs);
            const args = [].concat.apply([], values);
            const sql = `REPLACE INTO ${tablename} ${fs} VALUES ${lines.join(',')};`;
            const result = await dbSqlAsync(dbc, sql, args);
            result.op = 'replace_into';
            return result;
        } else {
            throw new Error(`DbTable<${this._cls}>: objects is not instanceof Array`);
        }
    }

    async upsertManyAsync(objects, duplicate_update = {key: "deleted", value: false}, strict = true) {
        /*
        @strict: strictForm
        @return: ResultSetHeader {
            fieldCount: 0,
            affectedRows: 2,
            insertId: 4,
            info: 'Records: 2  Duplicates: 2  Warnings: 0',
            serverStatus: 2,
            warningStatus: 0,
            },
         */
        const dup_form = Object.assign({}, duplicate_update);
        const key = dup_form.key;
        if (!key) {
            throw new Error(`DbTable<${this._cls}>: invalid duplicate_update(${JSON.stringify(duplicate_update)})`)
        }
        await this.ensureColumnsAsync(key);

        if (objects instanceof Array) {
            const count = objects.length;
            if (count === 0) {
                throw new Error(`DbTable<${this._cls}>: no objects to upsert`)
            }

            if (strict) {
                objects = objects.map(obj => this.constructor.strictForm(obj))
            }

            const {dbc, tablename} = this;
            const fields = Reflect.ownKeys(objects[0]);
            const values = objects.map(obj => fields.map(key => obj[key]));

            const fs = `(${fields.join(', ')})`;
            const vs = `(${fields.map(m => "?").join(', ')})`;
            const lines = new Array(count).fill(vs);
            const args = [].concat.apply([], values);

            let dup_on = `${dup_form.key} = ${dup_form.key}`;
            if (dup_form.value) {
                args.push(dup_form.value);
                dup_on = `${dup_form.key} = ?`;
            }
            const sql = `INSERT INTO ${tablename} ${fs} VALUES ${lines.join(',')} ON DUPLICATE KEY UPDATE ${dup_on};`;
            console.log(sql, args);
            const [result, cols] = await dbSqlAsync(dbc, sql, args);
            result.op = 'insert_ondup';
            return result;
        } else {
            throw new Error(`DbTable<${this._cls}>: objects is not instanceof Array`);
        }
    }

    async disableAsync(filter = {}) {
        // 软删除：require set field of "deleted"
        await this.ensureColumnsAsync("deleted");
        const updated_form = {deleted: true};
        const affectedRows = await this.updateAsync(filter, updated_form);
        return {op: "disable", state: affectedRows}
    }

    async enableAsync(filter = {}) {
        // 软恢复：require set field of "deleted"
        await this.ensureColumnsAsync("deleted");
        const updated_form = {deleted: false};
        const affectedRows = await this.updateAsync(filter, updated_form, false);
        return {op: "enable", state: affectedRows}
    }

    async deleteAsync(filter = {}) {
        // 硬删除：无法恢复
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(filter, false);
        const {query, args} = sqlFormat({eq: form});
        const sql = `DELETE FROM ${tablename} ${query};`;
        const [result, cols] = await await dbSqlAsync(dbc, sql, args);
        result.op = 'delete';
        return result
    }

}

module.exports = {
    _depends: {
        mysql_dbc
    },
    initDbc: initDbc,
    dbcPool: dbcPool,
    sqlFormat: sqlFormat,
    dbSqlAsync: dbSqlAsync,
    DbTable: DbTable,
};
