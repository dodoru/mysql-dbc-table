/*
*  Mysql Dbc Table:
*    A Simple Toolkit for Human to CRUD Table of Mysql.
*       - Simple Code but Flexible Usage.
*       - Fast predefine Column fields of table.
*       - Auto formatted($fmt) sql data to js object.(`$DbTable.fields()`)
*       - Restful CRUD Api to query Mysql table. (`$_field_flag_hidden`)
*       - Support Soft Deletion and Soft Recovery. (`$_field_flag_hidden`)
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

const isDbc = (dbc) => dbc instanceof Object && String(dbc._cls).startsWith("<MysqlDbc:") && String(dbc.uri).startsWith("mysql://");

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
    /*
    * <usage>: define column fields of DbTable, require override in SubClass of DbTable
    * options:
      `fmt` : <Function> : convert raw data from `node-mysql2` to javascript object.
      `default` : <value>: optional to init a row object to insert into mysql table.
    * return: {<$column>: {fmt: <$function>}}
    * sample:

        class User extends DbTable {
            static fields() {
                return {
                    id: {fmt: parseInt},
                    name: {fmt: String, default: ''},
                    created_time: {fmt: (...args) => new Date(...args)},
                    deleted: {fmt: Boolean, default: false},
                }
            }
        }

    * @_enable_delete_all: <Boolean: false> , avoid to delete all rows by `.deleteAsync()`
    * @_field_flag_hidden: <String: ""> , name of Primary-Column
    * @_field_flag_hidden: <String: ""> , name of Boolean-Column
    *   Table columns should be predefined by $DbTable.fields(), and Note that only One Or Zero field has this flag.
    *   This flag is designed for #SoftDeletion and #DisplayAfterReview,
    *   it would be automatically set in conditions while query table and hide deprecated rows.
    *   default: set column `deleted` to mark data to be hidden in common queries with default arguments.
    * */
    static fields() {
        this._enable_delete_all = false;
        this._field_primary_key = "id";
        this._field_flag_hidden = "deleted";
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
    * usage: new DbTable(<String:tablename>, <Dbc:db_connection>)
    *        new DbTable(<Dbc:db_connection>)
    * @tablename : <String>    ; optional, default is $classname
    * @dbc       : <DbcObject> ; required! if undefined try to init from $1:tablename_or_dbc
    * */
    constructor(tablename_or_dbc, dbc) {
        this._cls = `<DbTable:${this.constructor.name}>`;
        this.dbc = dbc || tablename_or_dbc;
        this.tablename = typeof (tablename_or_dbc) === "string" ? tablename_or_dbc : this.constructor.name;
        if (!isDbc(dbc)) {
            throw new Error(`${this._cls}[${this.tablename}]: init with invalid dbc ...`)
        }

        // checkout predefined fields
        this.fields = this.constructor.fields();
        this._enable_delete_all = this.constructor._enable_delete_all || false;
        this._field_primary_key = this.constructor._field_primary_key || "";
        this._field_flag_hidden = this.constructor._field_flag_hidden || "";
        if (this._field_flag_hidden) {
            let fg = this.fields[this._field_flag_hidden];
            if (!fg) {
                throw new Error(`${this._cls}[${this.tablename}]: invalid $_field_flag_hidden=${this._field_flag_hidden}`);
            }
            let tp = typeof (fg.fmt(false));
            if (tp !== "boolean") {
                throw new Error(`${this._cls}[${this.tablename}]: invalid $_field_flag_hidden=${this._field_flag_hidden}, required $fmt=>Boolean, not ${tp}`);
            }
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
    * <usage>: show column fields of Mysql Table
    * sample item of <Array:$cols> and <Array:$rows> from results of sql query.
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
    * return: <Array: [ <Object:{Field, Type, Key, Default, Extra}> ]>
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
    * return: <Array: [ <String:$ColumnName> ]>
    * */
    async listColumnNamesAsync() {
        const columns = await this.showColumnsAsync();
        return columns.map(m => m.Field);
    }

    /*
    * check <Array: $column_names>, raise Error if any column is not existed
    * return: Null
    * */
    async ensureColumnsAsync(...column_names) {
        const fields = await this.listColumnNamesAsync();
        const fields_set = new Set(fields);
        for (let column_name of column_names) {
            const is_existed = fields_set.has(column_name);
            if (!is_existed) {
                throw new Error(`${this._cls}[${this.tablename}] unknown field "${column_name}"`);
            }
        }
    }

    /*
    * return: <Object: {$column_field: $formatted_value} >
    * */
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

    /*
    * return: <Array: [ <Object: {$column_field: $formatted_value}> ]>
    * */
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

    /*
    * @conditions       : <Object : {$Column_field: $formatted_value}>
    *                   ; conditions to filter with equal fields value during `.queryAsync(<$qry.opts.eq>)`
    *                   ; suggest to set by result of `.strictForm()`
    * @ensureNotDeleted : <Boolean: default(true)>
    *                   ; hide deprecated items which were soft deleted by `.disableAsync()`
    * */
    static queryForm(conditions = {}, ensureNotDeleted = true) {
        const conds = Object.assign({}, conditions);
        if (typeof (conditions) !== "object") {
            throw Error(`<DbTable:${this.name}>: invalid $query=${conditions}, require <Object>`);
        }
        if (this._field_flag_hidden && ensureNotDeleted) {
            conds[this._field_flag_hidden] = false;
        }
        return conds;
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

    /*
    * @conditions       : <Object : {$Column_field: $formatted_value}>
    *                   ; conditions to filter with equal fields value during `.queryAsync(<$qry.opts.eq>)`
    *                   ; suggest to set by result of `.strictForm()`
    * @ensureNotDeleted : <Boolean: default(true)>
    *                   ; hide deprecated items which were soft deleted by `.disableAsync()`
    * */

    // return: <int: count>
    async countAsync(conditions = {}, ensureNotDeleted = true) {
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(conditions, ensureNotDeleted);
        const {query, args} = sqlFormat({eq: form});
        const res = this._field_primary_key || "*";
        const sql = `SELECT count(${res}) as count FROM ${tablename} ${query};`;
        const result = await dbSqlAsync(dbc, sql, args);
        const [rows, columns] = result;
        return rows[0].count;
    }

    // Return <Array: rows>:
    async findAsync(conditions = {}, ensureNotDeleted = true, res = '*', order = {}, limit) {
        const form = this.constructor.queryForm(conditions, ensureNotDeleted);
        const cond = {limit, order, res, opts: {eq: form}};
        return await this.queryAsync(cond);
    }

    // Return <Object: row>: random one with $filter, first one if $order.
    async findOneAsync(conditions = {}, ensureNotDeleted = true, res = '*', order = {}) {
        const limit = 1;
        const rows = await this.findAsync(conditions, ensureNotDeleted, res, order, limit);
        return rows[0] || null;
    }

    // Return exactly one result or Null,  raise an exception if find multiple rows.
    async getOrNullAsync(conditions = {}, ensureNotDeleted = true) {
        const rows = await this.findAsync(conditions, ensureNotDeleted, "*", {}, 2)
        if (rows.length === 0) {
            return null
        } else if (rows.length === 1) {
            return rows[0]
        } else {
            throw new Error(`${this._cls}[${this.tablename}]: conflict multiple rows on ${JSON.stringify(conditions)}`)
        }
    }

    // Return exactly one result, or raise an exception for zero or multiple rows.
    async getOr404Async(filter = {}, ensureNotDeleted = true) {
        const obj = await this.getOrNullAsync(filter, ensureNotDeleted);
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

    async updateAsync(conditions = {}, updated_form = {}, ensureNotDeleted = true) {
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(conditions, ensureNotDeleted);
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

    // update if existed else insert one
    // suggest to set conditions with primary key-value to UPSERT ONE
    async upsertAsync(conditions = {}, updated_form = {}) {
        const item = await this.findOneAsync(conditions, false);
        let op, state, data;
        if (item === null) {
            op = 'insert';
            data = Object.assign(conditions, updated_form);
            state = await this.addAsync(data);
        } else {
            if (item.deleted) {
                updated_form.deleted = false;
            }
            op = 'update';
            data = Object.assign({}, item, updated_form);
            state = await this.updateAsync(conditions, updated_form, false);
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
                throw new Error(`${this._cls}[${this.tablename}]: no objects to replace`)
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
            throw new Error(`${this._cls}[${this.tablename}]: require replaceManyAsync(<Array:[<objects>]>)`);
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
            throw new Error(`${this._cls}[${this.tablename}]: invalid duplicate_update(${JSON.stringify(duplicate_update)})`)
        }
        await this.ensureColumnsAsync(key);

        if (objects instanceof Array) {
            const count = objects.length;
            if (count === 0) {
                throw new Error(`${this._cls}[${this.tablename}]: no objects to upsert`)
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
            throw new Error(`${this._cls}[${this.tablename}]: require upsertManyAsync(<Array:[<objects>]>)`);
        }
    }


    async disableAsync(conditions = {}) {
        // #SoftDeletion: #软删除：require set $_field_flag_hidden
        if (!this._field_flag_hidden) {
            throw Error(`${this._cls}[${this.tablename}]: unset $_field_flag_hidden`)
        }
        const updated_form = {[this._field_flag_hidden]: true};
        const affectedRows = await this.updateAsync(conditions, updated_form);
        return {op: "disable", state: affectedRows}
    }

    async enableAsync(conditions = {}) {
        // #SoftRecovery: #软恢复：require set $_field_flag_hidden
        if (!this._field_flag_hidden) {
            throw Error(`${this._cls}[${this.tablename}]: unset $_field_flag_hidden`)
        }
        const updated_form = {[this._field_flag_hidden]: false};
        const affectedRows = await this.updateAsync(conditions, updated_form, false);
        return {op: "enable", state: affectedRows}
    }

    async deleteAsync(conditions = {}) {
        // #HardDeletion: #硬删除：never recovery (无法恢复)
        if (!this._enable_delete_all && Object.keys(conditions).length === 0) {
            let msgs = [
                "you are trying to delete all the rows , it\'s too dangerous !!!",
                "If you are determined to delete all, you should manually set `this._enable_delete_all = true`.",
            ]
            throw Error(`${this._cls}[${this.tablename}]: ${msgs.join('\n')} `)
        }
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(conditions, false);
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
