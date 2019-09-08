class SqlError extends Error {
    constructor(message, errno) {
        super();
        this.message = message;
        this.errno = errno || 40099;
    }
}

class SqlArgsError extends SqlError {
    constructor(message, errno) {
        super();
        this.message = message;
        this.errno = 40097;
    }
}

class SqlMultipleResultsFound extends SqlError {
    constructor(message, errno) {
        super();
        this.message = message;
        this.errno = 40095;
    }
}

class SqlDataNotFound extends SqlError {
    constructor(message, errno) {
        super();
        this.message = message;
        this.errno = 40490;
    }
}

class SqlDbcNotFound extends SqlError {
    constructor(dbc_name) {
        super();
        this.message = `[DbcPool] not found dbc<${dbc_name}>`;
        this.errno = 50094;
    }
}


module.exports = {
    SqlError: SqlError,
    SqlArgsError: SqlArgsError,
    SqlDbcNotFound: SqlDbcNotFound,
    SqlDataNotFound: SqlDataNotFound,
    SqlMultipleResultsFound: SqlMultipleResultsFound,
}
