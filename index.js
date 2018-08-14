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
        let q = fs.map(key => ` ${key} ${op} ? `).join(' AND ');
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
    limit = parseInt(limit);
    if (!isNaN(limit) && limit > 0) {
        query += ' LIMIT ? ';
        args.push(limit);
    }
    return {query, args};
};


const dbSqlSync = async (dbc, sql, args) => {
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

    async sqlSync(sql, args) {
        const {dbc, tablename} = this;
        return await dbSqlSync(dbc, sql, args);
    }

    async selectSync(sql, args) {
        const {dbc, tablename} = this;
        const func = dbc.withConnection(
            function () {
                return this.doSelect(sql, args)
            }
        );
        return await func();
    }

    async countSync(filter = {}, ensureNotDeleted) {
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(filter, ensureNotDeleted);
        const {query, args} = sqlFormat({eq: form});
        const sql = `SELECT count(*) as count FROM ${tablename} ${query};`;
        const result = await dbSqlSync(dbc, sql, args);
        const [rows, columns] = result;
        return rows[0].count;
    }


    async findSync(filter = {}, ensureNotDeleted) {
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(filter, ensureNotDeleted);
        const fields = Reflect.ownKeys(form);
        const values = fields.map(key => form[key]);
        const func = dbc.withConnection(
            function () {
                return this.selectManyByFields(tablename, fields, values)
            }
        );
        return await func();
    }

    async findOneSync(filter = {}, ensureNotDeleted) {
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(filter, ensureNotDeleted);
        const func = dbc.withConnection(
            function () {
                return this.selectOneByObject(tablename, form);
            }
        );
        return await func();
    }

    async findLimitSync(limit = 1, filter = {}, order = {}, ensureNotDeleted) {
        // size: int : 个数
        const {dbc, tablename} = this;
        const form = this.constructor.queryForm(filter, ensureNotDeleted);
        const {query, args} = sqlFormat({eq: form}, order, limit);
        const sql = `SELECT * from ${tablename} ${query} ;`;
        return await this.selectSync(sql, args);
    }

    async findOneByFieldsSync(field_keys, field_values) {
        const {dbc, tablename} = this;
        const func = dbc.withConnection(
            function () {
                return this.selectOneByFields(tablename, field_keys, field_values)
            }
        );
        return await func();
    }

    async addSync(object) {
        const {dbc, tablename} = this;
        const func = dbc.withConnection(
            function () {
                return this.insertOneObject(tablename, object);
            }
        );
        return await func();
    }

    async addManySync(objects) {
        const {dbc, tablename} = this;
        const func = dbc.withConnection(
            function () {
                return this.insertManyObjects(tablename, objects);
            }
        );
        return await func();
    }

    async updateSync(filter = {}, updated_form = {}, ensureNotDeleted) {
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

    async ensureSync(object) {
        const item = await this.findOneSync(object, false);
        if (item === null) {
            return await this.addSync(object);
        } else {
            if (item.deleted) {
                return await this.reviveSync(item);
            }
        }
    }

    async upsertSync(filter = {}, updated_form = {}) {
        const item = await this.findOneSync(filter, false);
        let op, state, data;
        if (item === null) {
            op = 'insert';
            data = Object.assign(filter, updated_form);
            state = await this.addSync(data);
        } else {
            if (item.deleted) {
                updated_form.deleted = false;
            }
            op = 'update';
            data = Object.assign({}, item, updated_form);
            state = await this.updateSync(filter, updated_form, false);
        }
        return {op, data, state}
    }

    async delSync(filter = {}) {
        const updated_form = {deleted: true};
        return await this.updateSync(filter, updated_form);
    }

    async reviveSync(filter = {}) {
        const updated_form = {deleted: false};
        return await this.updateSync(filter, updated_form, false);
    }
}

module.exports = {
    _depends: {
        mysql_dbc
    },
    initDbc: initDbc,
    dbcPool: dbcPool,
    sqlFormat: sqlFormat,
    dbSqlSync: dbSqlSync,
    DbTable: DbTable,
};
