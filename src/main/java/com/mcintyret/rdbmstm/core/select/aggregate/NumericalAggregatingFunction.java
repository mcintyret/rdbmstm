package com.mcintyret.rdbmstm.core.select.aggregate;

import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import com.mcintyret.rdbmstm.core.ColumnDefinition;
import com.mcintyret.rdbmstm.core.DataType;
import com.mcintyret.rdbmstm.core.Tuple;
import com.mcintyret.rdbmstm.core.Value;

abstract class NumericalAggregatingFunction implements AggregatingFunction {

    private static final ColumnDefinition COLUMN_DEFINITION = new ColumnDefinition(DataType.FLOAT);

    @Override
    public Value aggregate(String columnName, Stream<? extends Tuple> tuples) {
        return Value.of(toValue(tuples.mapToDouble(tuple -> toDouble(tuple, columnName))));
    }

    private static double toDouble(Tuple tuple, String columnName) {
        return ((Number) tuple.select(columnName).getValue()).doubleValue();
    }

    protected abstract double toValue(DoubleStream doubleStream);

    @Override
    public ColumnDefinition getColumnDefinition() {
        return COLUMN_DEFINITION;
    }
}
