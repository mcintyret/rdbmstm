package com.mcintyret.rdbmstm.core;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.mcintyret.rdbmstm.SqlException;

public abstract class Row implements Tuple {

    @Override
    public Value select(String columnName) throws SqlException {
        checkColumnName(columnName);
        return getValues().get(columnName);
    }

    @Override
    public void set(String columnName, Value value) throws SqlException {
        ColumnDefinition colDef = getColumnDefinitions().get(columnName);
        if (colDef == null) {
            throw new SqlException("No such column: " + columnName);
        } else if (colDef.getDataType() != value.getDataType()) {
            throw new SqlException("Value of type " + value.getDataType() + " cannot be placed in column '"
            + columnName + "' of type " + colDef.getDataType());
        } else if (value.isNull() && !colDef.isNullable()) {
            throw new SqlException("Cannot set NULL value on non-NULLABLE column '" + columnName + "'");
        }
        getValues().put(columnName, value);
    }

    private void checkColumnName(String columnName) throws SqlException {
        if (!getColumnDefinitions().containsKey(columnName)) {
            throw new SqlException("No such column: " + columnName);
        }
    }

    protected abstract Map<String, Value> getValues();

    @Override
    public Iterator<Value> iterator() {
        return getValues().values().iterator();
    }


    @Override
    // TODO: is ordering important to equals / hashCode?
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Row tuple = (Row) o;

        return getValues().equals(tuple.getValues());

    }

    @Override
    // TODO: is ordering important to equals / hashCode?
    public int hashCode() {
        return getValues().hashCode();
    }


    public Row copy() {
        final Map<String, Value> values = new LinkedHashMap<>(getValues());
        return new Row() {
            @Override
            public Map<String, ColumnDefinition> getColumnDefinitions() {
                return Row.this.getColumnDefinitions();
            }

            @Override
            public Map<String, Value> getValues() {
                return values;
            }
        };
    }
}
