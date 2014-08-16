package com.mcintyret.rdbmstm.core.predicate;

import com.mcintyret.rdbmstm.core.Value;

public class ColumnLessThan extends NumericColumnPredicate {

    public ColumnLessThan(String columnName, Value val) {
        super(columnName, val);
    }

    @Override
    protected boolean doTest(double v, double test) {
        return v < test;
    }
}
