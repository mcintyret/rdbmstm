package com.mcintyret.rdbmstm.core;

public enum DataType {
    INTEGER,
    FLOAT,
    STRING,
    DATETIME,
    BOOLEAN;

    public boolean isNumeric() {
        return this == INTEGER || this == FLOAT;
    }
}
