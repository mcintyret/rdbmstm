package com.mcintyret.rdbmstm.core.predicate;

public class ColumnGreaterThan extends NumericColumnPredicate {

    public ColumnGreaterThan(String columnName, double test) {
        super(columnName, test);
    }

    @Override
    protected boolean doTest(double v, double test) {
        return v > test;
    }
}
