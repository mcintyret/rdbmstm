package com.mcintyret.rdbmstm.core;

import java.util.HashMap;
import java.util.Map;

import com.mcintyret.rdbmstm.SqlException;

public class Index {

    private final String columnName;

    private final Map<Value, Row> map = new HashMap<>();

    public Index(String columnName) {
        this.columnName = columnName;
    }

    public void add(Row tuple) {
        Value val = tuple.getValues().get(columnName);
        if (map.putIfAbsent(val, tuple) != null) {
            throw new SqlException("Unique Key constraint violation: Duplicate value for column '" + columnName + "': " + val);
        }
    }

    public void remove(Row tuple) {
        map.remove(tuple.getValues().get(columnName));
    }
}
