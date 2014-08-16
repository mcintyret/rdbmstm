package com.mcintyret.rdbmstm.core;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map;

public abstract class Tuple extends AbstractCollection<Value> {

    public Value select(String columnName) {
        checkColumnName(columnName);
        return getValues().get(columnName);
    }

    private void checkColumnName(String columnName) {
        if (!getColumnDefinitions().containsKey(columnName)) {
            throw new IllegalArgumentException("No such column: " + columnName);
        }
    }

    public void set(String columnName, Value value) {
        DataType colType = getColumnDefinitions().get(columnName);
        if (colType == null) {
            throw new IllegalArgumentException("No such column: " + columnName);
        } else if (colType != value.getDataType()) {
            throw new IllegalArgumentException("Value of type " + value.getDataType() + " cannot be placed in column '"
            + columnName + "' of type " + colType);
        }
        getValues().put(columnName, value);
    }


    public abstract Map<String, DataType> getColumnDefinitions();

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
