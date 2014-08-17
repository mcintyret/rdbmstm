package com.mcintyret.rdbmstm.core.select.aggregate;

import java.util.stream.Stream;

import com.mcintyret.rdbmstm.core.ColumnDefinition;
import com.mcintyret.rdbmstm.core.DataType;
import com.mcintyret.rdbmstm.core.Tuple;
import com.mcintyret.rdbmstm.core.Value;

public class Count implements AggregatingFunction {

    private static final ColumnDefinition COLUMN_DEFINITION = new ColumnDefinition(DataType.INTEGER);

    @Override
    public Value aggregate(String columnName, Stream<? extends Tuple> tuples) {
        return Value.of(tuples.count());
    }

    @Override
    public ColumnDefinition getColumnDefinition() {
        return COLUMN_DEFINITION;
    }

    @Override
    public String getName() {
        return "count";
    }
}
