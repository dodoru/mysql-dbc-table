const SqlArgsError = require('./sql_errors').SqlArgsError;

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
    is: 'IS',
    is_not: 'IS NOT',
};

const _isNaN = (value) => {
    return type(value) === "number" && isNaN(value)
}

const optFilter = (form) => {
    // return: {is, is_not, eq}
    const opts = {}
    for (let key in form) {
        let value = form[key];
        let item = {[key]: value};
        if (_isNaN(value)) {
            throw new SqlArgsError(`invalid ${key}=${value}, require number`)
        }
        if (value === null) {
            opts.is = Object.assign(item, opts.is)
        } else {
            // 注意 undefined 对应的是 not NULL
            if (value === undefined) {
                item = {[key]: null};
                opts.is_not = Object.assign(item, opts.is_not);
            } else {
                opts.eq = Object.assign(item, opts.eq)
            }
        }
    }
    return opts;
}

const sqlFormat = (opts = {}, order = {}, limit) => {
    const qs = [];
    const args = [];

    for (let mark in opts) {
        const op = K_OP[mark];
        if (op === undefined) {
            throw new SqlArgsError(`invalid mysql.OP<${mark}>`)
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


module.exports = {
    K_OP: K_OP,
    optFilter: optFilter,
    sqlFormat: sqlFormat,
}
