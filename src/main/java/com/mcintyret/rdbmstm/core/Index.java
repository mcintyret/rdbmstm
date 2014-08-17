package com.mcintyret.rdbmstm.core;

import java.util.HashMap;
import java.util.Map;

import com.mcintyret.rdbmstm.SqlException;

public class Index {

    private final String columnName;

    private final Map<Value, Tuple> map = new HashMap<>();

    public Index(String columnName) {
        this.columnName = columnName;
    }

    public void add(Tuple tuple) {
        Value val = tuple.select(columnName);
        if (map.putIfAbsent(val, tuple) != null) {
            throw new SqlException("Unique Key constraint violation: Duplicate value for column '" + columnName + "': " + val);
        }
    }

    public void remove(Tuple tuple) {
        map.remove(tuple.select(columnName));
    }
}
