const fmtJson = (m) => {
    const tp = typeof (m);
    switch (tp) {
        case 'string':
            return JSON.parse(m);
        case 'object':
            return m;
        default:
            return m || null;
    }
};

const trimString = (m) => {
    if (m === undefined || m === null) {
        return ''
    }
    if (typeof (m) === 'string') {
        return m.trim()
    }
    if (typeof (m) === 'number') {
        return String(m)
    }
    throw new Error('trimString: value should be undefined, null， string, or number')
}

const enumFalseSet = new Set(["", "false", "0", "undefined", "null", "none", "[]", "{}"])

const varBool = (m) => {
    if (typeof (m) === 'string') {
        m = m.trim().toLowerCase()
        if (enumFalseSet.has(m)) {
            return false
        }
    }
    if (m instanceof Array) {
        return Boolean(m.length > 0)
    }
    if (typeof (m) === "object") {
        return Boolean(Object.keys(m).length > 0)
    }
    return Boolean(m)
}

const varIntU = (m) => {
    let v = parseInt(m)
    if (isNaN(v)) {
        return undefined;
    }
    return v;
}

module.exports = {
    fmtJson: fmtJson,
    trimString: trimString,
    varBool: varBool,
    varIntU: varIntU,
};