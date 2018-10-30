/*
*  Mysql Db Table
*  统一数据库的接口，尽量不要裸写 mysql 语句
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
    }

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
    constructor(tablename, dbc) {
        this.tablename = tablename;
        if (dbc && dbc instanceof Object) {
            // todo: 更严谨的判定
            this.dbc = dbc;
        } else {
            throw new Error('[mysql], init DbTable with invalid dbc ...')
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
        // require super in subclass
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
        const dbc = this.dbc;
        const tablename = this.tablename;
        const sql = `show columns from ${tablename}`;
        const result = await dbSqlAsync(dbc, sql);
        const [rows, cols] = result;
        return rows;
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
        const res = qry.res || '*';
        const opts = qry.opts || {};
        const order = qry.order || {};
        const limit = qry.limit;
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
                return await this.reviveAsync(item);
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

    async delAsync(filter = {}) {
        // 软删除：if ('deleted' in this.constructor.fields())
        const updated_form = {deleted: true};
        return await this.updateAsync(filter, updated_form);
    }

    async reviveAsync(filter = {}) {
        // 软恢复：if ('deleted' in this.constructor.fields())
        const updated_form = {deleted: false};
        return await this.updateAsync(filter, updated_form, false);
    }

    async removeAsync(filter = {}) {
        // 硬删除：无法恢复
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(filter, false);
        const {query, args} = sqlFormat({eq: form});
        const sql = `DELETE FROM ${tablename} ${query};`;
        return await await dbSqlAsync(dbc, sql, args);
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
