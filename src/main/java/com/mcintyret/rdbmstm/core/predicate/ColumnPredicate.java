package com.mcintyret.rdbmstm.core.predicate;

abstract class ColumnPredicate implements TuplePredicate {

    final String columnName;

    protected ColumnPredicate(String columnName) {
        this.columnName = columnName;
    }
}
