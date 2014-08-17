package com.mcintyret.rdbmstm.core;

import java.util.HashMap;
import java.util.Map;

import com.mcintyret.rdbmstm.SqlException;
import com.mcintyret.rdbmstm.core.select.WritableTuple;

public abstract class Row extends AbstractTuple implements WritableTuple {

    private final Map<String, Value> values = new HashMap<>();

    public Row copy() {
        Row copy = new Row() {
            @Override
            public Map<String, ColumnDefinition> getColumnDefinitions() {
                return Row.this.getColumnDefinitions();
            }
        };
        copy.values.putAll(values);
        return copy;
    }

    @Override
    protected Value doSelect(String colName) {
        return values.get(colName);
    }

    @Override
    public final void set(String columnName, Value value) {
        ColumnDefinition colDef = getColumnDefinitions().get(columnName);
        if (colDef == null) {
            throw new SqlException("No such column: " + columnName);
        } else if (colDef.getDataType() != value.getDataType()) {
            throw new SqlException("Value of type " + value.getDataType() + " cannot be placed in column '"
                + columnName + "' of type " + colDef.getDataType());
        } else if (value.isNull() && !colDef.isNullable()) {
            throw new SqlException("Cannot set NULL value on non-NULLABLE column '" + columnName + "'");
        }

        values.put(columnName, value);
    }

}
