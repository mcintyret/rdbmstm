package com.mcintyret.rdbmstm.core.predicate;

import java.util.Objects;

import com.mcintyret.rdbmstm.core.Tuple;
import com.mcintyret.rdbmstm.core.Value;

public class ColumnEquals extends ColumnPredicate {

    private Value value;

    public ColumnEquals(String columnName, Value value) {
        super(columnName);
        this.value = value;
    }

    @Override
    public boolean test(Tuple tuple) {
        return Objects.equals(value, tuple.select(columnName));
    }
}
