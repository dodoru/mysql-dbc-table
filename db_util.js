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

const trimString = (value) => {
    // :value: undefined, null，string, or number
    // return: string
    if (value === undefined || value === null) {
        return ''
    }
    if (typeof (value) === 'string') {
        return value.trim()
    }
    if (typeof (value) === 'number') {
        return String(value)
    }
    throw new Error('TypeError: $trimString expect value of undefined, null，string, or number')
}

const enumFalseSet = new Set(["", "false", "0", "undefined", "null", "none", "[]", "{}"])

const varStringMap = {
    undefined: undefined,
    null: null,
    none: null,
}

const varStringSet = new Set(Object.keys(varStringMap))

const varStr = (value) => {
    // :value: undefined, null，string, or number
    // return: undefined, null，string, or number
    if (value === undefined || value === null) {
        return value
    }
    if (typeof (value) === "number") {
        return String(value)
    }
    if (typeof (value) === 'string') {
        let val = value.trim()
        let key = val.toLowerCase()
        if (varStringSet.has(key)) {
            return varStringMap[key]
        } else {
            return val;
        }
    }
    throw new Error('TypeError: $varStr expect value of undefined, null，string, or number')
};

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
    varStringMap: varStringMap,
    varStringSet: varStringSet,
    varStr: varStr,
    varBool: varBool,
    varIntU: varIntU,
};