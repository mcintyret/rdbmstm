package com.mcintyret.rdbmstm.core.predicate;

import com.mcintyret.rdbmstm.SqlException;
import com.mcintyret.rdbmstm.core.DataType;
import com.mcintyret.rdbmstm.core.Tuple;

public class ColumnMatchesRegex extends ColumnPredicate {

    private final String regex;

    ColumnMatchesRegex(String columnName, String regex) {
        super(columnName);
        this.regex = regex;
    }

    @Override
    public boolean test(Tuple tuple) {
        if (tuple.getColumnDefinitions().get(columnName) != DataType.STRING) {
            throw new SqlException("Regex predicate only valid for values of type String");
        }
        return tuple.select(columnName).toString().matches(regex);
    }
}
