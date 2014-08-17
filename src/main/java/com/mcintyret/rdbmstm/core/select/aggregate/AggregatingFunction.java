package com.mcintyret.rdbmstm.core.select.aggregate;

import java.util.stream.Stream;

import com.mcintyret.rdbmstm.core.ColumnDefinition;
import com.mcintyret.rdbmstm.core.Tuple;
import com.mcintyret.rdbmstm.core.Value;

public interface AggregatingFunction {

    Value aggregate(String columnName, Stream<? extends Tuple> tuples);

    ColumnDefinition getColumnDefinition();

    String getName();

    default String getColumnName(String columnName) {
        return getName() + "(" + columnName + ")";
    }

}
