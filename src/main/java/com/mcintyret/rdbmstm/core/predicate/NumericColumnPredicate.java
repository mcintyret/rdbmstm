package com.mcintyret.rdbmstm.core.predicate;

import com.mcintyret.rdbmstm.SqlException;
import com.mcintyret.rdbmstm.core.DataType;
import com.mcintyret.rdbmstm.core.Tuple;

abstract class NumericColumnPredicate extends ColumnPredicate {

    private final double test;

    protected NumericColumnPredicate(String columnName, double test) {
        super(columnName);
        this.test = test;
    }

    @Override
    public boolean test(Tuple tuple) {
        checkDataType(tuple.getColumnDefinitions().get(columnName).getDataType());
        return doTest(((Number) tuple.select(columnName).getValue()).doubleValue(), test);
    }

    protected abstract boolean doTest(double v, double test);

    private void checkDataType(DataType dataType) {
        if (!dataType.isNumeric()) {
            throw new SqlException(getClass().getSimpleName() + " only valid for numeric columns");
        }
    }
}
