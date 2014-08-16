package com.mcintyret.rdbmstm.core;

import com.mcintyret.rdbmstm.SqlException;

public class ColumnDefinition {

    private final DataType dataType;

    private final boolean nullable;

    private final boolean unique;

    public ColumnDefinition(DataType dataType) {
        // Note: not nullable by default!
        this(dataType, false, false);
    }

    public ColumnDefinition(DataType dataType, boolean nullable, boolean unique) {
        if (nullable && unique) {
            throw new SqlException("Column can't be both NULLABLE and UNIQUE");
        }
        this.dataType = dataType;
        this.nullable = nullable;
        this.unique = unique;
    }

    public Value getDefaultValue() {
        return Value.nullOf(dataType);
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isUnique() {
        return unique;
    }
}
