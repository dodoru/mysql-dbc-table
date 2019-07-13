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
        this.errno = errno || 40097;
    }
}


module.exports = {
    SqlError: SqlError,
    SqlArgsError: SqlArgsError,
}