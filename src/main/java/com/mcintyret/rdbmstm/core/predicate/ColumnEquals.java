package com.mcintyret.rdbmstm.core.predicate;

import java.util.Objects;

import com.mcintyret.rdbmstm.core.Tuple;

public class ColumnEquals extends ColumnPredicate {

    private Object value;

    ColumnEquals(String columnName, Object value) {
        super(columnName);
        this.value = value;
    }

    @Override
    public boolean test(Tuple tuple) {
        return Objects.equals(value, tuple.select(columnName));
    }
}
