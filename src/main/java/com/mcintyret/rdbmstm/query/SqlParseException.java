package com.mcintyret.rdbmstm.query;

import com.mcintyret.rdbmstm.SqlException;

public class SqlParseException extends SqlException {

    SqlParseException(String message) {
        super(message);
    }

    SqlParseException(String message, Throwable cause) {
        super(message, cause);
    }

}
