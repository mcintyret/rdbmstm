package com.mcintyret.rdbmstm.core.predicate;

public class ColumnLessThan extends NumericColumnPredicate {

    public ColumnLessThan(String columnName, double test) {
        super(columnName, test);
    }

    @Override
    protected boolean doTest(double v, double test) {
        return v < test;
    }
}
