package com.mcintyret.rdbmstm.core;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map;

import com.mcintyret.rdbmstm.SqlException;

public abstract class Tuple extends AbstractCollection<Value> {

    public Value select(String columnName) throws SqlException {
        checkColumnName(columnName);
        return getValues().get(columnName);
    }

    private void checkColumnName(String columnName) throws SqlException {
        if (!getColumnDefinitions().containsKey(columnName)) {
            throw new SqlException("No such column: " + columnName);
        }
    }

    public void set(String columnName, Value value) throws SqlException {
        DataType colType = getColumnDefinitions().get(columnName).getDataType();
        if (colType == null) {
            throw new SqlException("No such column: " + columnName);
        } else if (colType != value.getDataType()) {
            throw new SqlException("Value of type " + value.getDataType() + " cannot be placed in column '"
            + columnName + "' of type " + colType);
        }
        getValues().put(columnName, value);
    }


    public abstract Map<String, ColumnDefinition> getColumnDefinitions();

    public abstract Map<String, Value> getValues();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple tuple = (Tuple) o;

        return getValues().equals(tuple.getValues());

    }

    @Override
    public int hashCode() {
        return getValues().hashCode();
    }

    @Override
    public int size() {
        return getValues().size();
    }

    @Override
    public Iterator<Value> iterator() {
        return getValues().values().iterator();
    }
}
