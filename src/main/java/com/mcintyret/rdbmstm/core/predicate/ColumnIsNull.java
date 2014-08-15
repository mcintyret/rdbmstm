package com.mcintyret.rdbmstm.core.predicate;

import com.mcintyret.rdbmstm.core.Tuple;

public class ColumnIsNull extends ColumnPredicate {

    public ColumnIsNull(String columnName) {
        super(columnName);
    }

    @Override
    public boolean test(Tuple tuple) {
        return tuple.select(columnName) == null;
    }
}
