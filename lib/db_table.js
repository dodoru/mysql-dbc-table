/*
*  Mysql Db Table
*  统一数据库的接口，尽量不要裸写 mysql 语句
* */

const mysql_dbc = require('mysql-dbc');
const sql_errors = require("./sql_errors");
const {optFilter, sqlFormat} = require('./sql_format');
const DbcName = "mysql-dbc";

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
    dbc._cls = DbcName;
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
    logger: console,
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
                throw new sql_errors.SqlDbcNotFound(name)
            }
        }
        return dbc;
    },
    getPool: () => {
        return dbcPool._pools;
    },
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
    constructor(tablename, dbc) {
        this._cls = this.constructor.name;
        this.tablename = tablename;
        if (dbc && dbc instanceof Object && dbc._cls === DbcName) {
            // todo: 更严谨的判定
            this.dbc = dbc;
        } else {
            throw new sql_errors.SqlArgsError(`[DbTable:${this._cls}:${tablename}], init DbTable with invalid dbc ...`)
        }
    }

    info() {
        const model = this.constructor.name;
        const tablename = this.tablename;
        const {host, port, user, database} = this.dbc.config;
        return {model, tablename, database, host, port, user}
    }

    toString() {
        const {model, tablename, database, host, port, user} = this.info();
        return `[DbTable:${model}:${tablename}] dbc=${user}@${host}:${port}/${database}`;
    }

    toJSON() {
        return this.toString();
    }

    static fields() {
        // require rewrite in subclass
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

    async showColumnsAsync() {
        /*
         samples of rows and cols
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
         },
         row = TextRow {
            Field: 'id',
            Type: 'int(11)',
            Null: 'NO',
            Key: 'PRI',
            Default: null,
            Extra: 'auto_increment'
         },
        * */
        const dbc = this.dbc;
        const tablename = this.tablename;
        const sql = `show columns from ${tablename}`;
        const result = await dbSqlAsync(dbc, sql);
        const [rows, cols] = result;
        return rows;
    }

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
                throw new sql_errors.SqlArgsError(`DbTable<${this._cls}> unknown field "${column_name}"`);
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
                let v = fmt(value);
                // cond: undefined is invalid
                if (v !== undefined) {
                    if (typeof (v) === "number" && isNaN(v)) {
                        throw new sql_errors.SqlArgsError(`invalid ${key}=${value}, require number`)
                    } else {
                        form[key] = v;
                    }
                }
            }
        }
        return form;
    }

    static queryForm(cond, ensureNotDeleted) {
        const form = Object.assign({}, cond);
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
        let res = qry.res || '*';
        const opts = qry.opts || {};
        const order = qry.order || {};
        const limit = qry.limit;
        const {dbc, tablename} = this;
        const {query, args} = sqlFormat(opts, order, limit);
        if (res === "*") {
            let fds = this.constructor.fields();
            res = Object.keys(fds).join(",");
        }
        const sql = `SELECT ${res} FROM ${tablename} ${query}; `;
        const func = dbc.withConnection(
            function () {
                return this.doSelect(sql, args)
            }
        );
        return await func();
    }

    async findAsync(cond = {}, ensureNotDeleted, res = '*', order = {}, limit) {
        const form = this.constructor.queryForm(cond, ensureNotDeleted);
        const opts = optFilter(form);
        const qry = {opts, res, order, limit}
        return await this.queryAsync(qry);
    }

    async findOneAsync(cond = {}, ensureNotDeleted, res = '*', order = {}) {
        const rows = await this.findAsync(cond, ensureNotDeleted, res, order, 2)
        if (rows.length <= 1) {
            return rows[0] || null
        }
        throw new SqlError(`[ConflictItems] <${this.tablename}:${JSON.stringify(cond)}' expect One Or None , but get rows >= 2 :\n ${ JSON.stringify(rows, null, 2)} !!! `)
    }

    async countAsync(cond = {}, ensureNotDeleted) {
        const res = 'count(*) as count';
        const row = await this.findOneAsync(cond, ensureNotDeleted, res);
        return row.count;
    }

    async getOr404Async(cond = {}, ensureNotDeleted, res = '*') {
        const obj = await this.findOneAsync(cond, ensureNotDeleted, res);
        if (obj === null) {
            const e = new SqlError(`[DataNotFound] <${this.tablename}:${JSON.stringify(cond)}>`);
            e.status = 404;
            e.errno = 40400;
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

    async updateAsync(cond = {}, updated_form = {}, ensureNotDeleted) {
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(cond, ensureNotDeleted);
        const cond_fields = Reflect.ownKeys(form);
        const cond_values = cond_fields.map(key => form[key]);

        const updated_fields = Reflect.ownKeys(updated_form);
        const updated_values = updated_fields.map(key => updated_form[key]);
        if (updated_fields.length === 0) {
            return 0
        } else {
            const fws = cond_fields.map(k => `${k} = ?`).join(' AND ');
            const ups = updated_fields.map(k => `${k} = ?`).join(', ');
            const sql = `UPDATE ${tablename} SET ${ups} WHERE ${fws}`;
            const func = dbc.withConnection(
                function () {
                    return this.doUpdate(sql, [...updated_values, ...cond_values])
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

    async upsertAsync(cond = {}, updated_form = {}) {
        const item = await this.findOneAsync(cond, false);
        let op, state, data;
        if (item === null) {
            op = 'insert';
            data = Object.assign(cond, updated_form);
            state = await this.addAsync(data);
        } else {
            if (item.deleted) {
                updated_form.deleted = false;
            }
            op = 'update';
            data = Object.assign({}, item, updated_form);
            state = await this.updateAsync(cond, updated_form, false);
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
                throw new sql_errors.SqlArgsError(`DbTable<${this._cls}>: no objects to update`)
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
            throw new sql_errors.SqlArgsError(`DbTable<${this._cls}>:  objects to replace must be Array`);
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
            throw new sql_errors.SqlArgsError(`DbTable<${this._cls}>: duplicate_update(${JSON.stringify(duplicate_update)})`)
        }
        await this.ensureColumnsAsync(key);

        if (objects instanceof Array) {
            const count = objects.length;
            if (count === 0) {
                throw new sql_errors.SqlArgsError(`DbTable<${this._cls}>: no objects to upsert`)
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
            // console.log(sql, args);
            const [result, cols] = await dbSqlAsync(dbc, sql, args);
            result.op = 'insert_ondup';
            return result;
        } else {
            throw new sql_errors.SqlArgsError(`DbTable<${this._cls}>: objects is not instanceof Array`);
        }
    }

    async disableAsync(cond = {}) {
        // 软删除：require set field of "deleted"
        await this.ensureColumnsAsync("deleted");
        const updated_form = {deleted: true};
        const affectedRows = await this.updateAsync(cond, updated_form);
        return {op: "disable", state: affectedRows}
    }

    async enableAsync(cond = {}) {
        // 软恢复：require set field of "deleted"
        await this.ensureColumnsAsync("deleted");
        const updated_form = {deleted: false};
        const affectedRows = await this.updateAsync(cond, updated_form, false);
        return {op: "enable", state: affectedRows}
    }

    async deleteAsync(cond = {}) {
        // 硬删除：无法恢复
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(cond, false);
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
