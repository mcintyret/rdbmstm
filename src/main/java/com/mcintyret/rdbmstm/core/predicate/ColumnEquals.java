package com.mcintyret.rdbmstm.core.predicate;

import java.util.Objects;

import com.mcintyret.rdbmstm.core.DataType;
import com.mcintyret.rdbmstm.core.Tuple;
import com.mcintyret.rdbmstm.core.Value;

public class ColumnEquals extends ColumnPredicate {

    private String value;

    public ColumnEquals(String columnName, String value) {
        super(columnName);
        this.value = value;
    }

    @Override
    public boolean test(Tuple tuple) {
        DataType type = tuple.getColumnDefinitions().get(columnName).getDataType();
        return Objects.equals(tuple.select(columnName), Value.parse(value, type));
    }
}
