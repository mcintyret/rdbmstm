package com.mcintyret.rdbmstm.core;

import java.util.LinkedHashMap;

public class ColumnDefinitions extends LinkedHashMap<String, DataType> {

    public ColumnDefinitions readOnlySubset(Iterable<String> toKeep) {
        ColumnDefinitions subset = new ColumnDefinitions();
        for (String col : toKeep) {
            DataType dt = get(col);
            if (dt == null) {
                throw new AssertionError("No such column: " + col);
            }
            subset.put(col, dt);
        }
        return subset;
    }

}
