package com.mcintyret.rdbmstm.core;

import java.util.Iterator;

import com.mcintyret.rdbmstm.SqlException;
import com.mcintyret.rdbmstm.collect.Iterators;

public abstract class AbstractTuple implements Tuple {

    private void checkColumnName(String columnName) throws SqlException {
        if (!getColumnDefinitions().containsKey(columnName)) {
            throw new SqlException("No such column: " + columnName);
        }
    }

    @Override
    public final Value select(String colName) {
        checkColumnName(colName);
        return doSelect(colName);
    }

    protected abstract Value doSelect(String colName);

    @Override
    public Iterator<Value> iterator() {
        return new Iterator<Value>() {
            final Iterator<String> colIt = getColumnDefinitions().keySet().iterator();

            @Override
            public boolean hasNext() {
                return colIt.hasNext();
            }

            @Override
            public Value next() {
                return doSelect(colIt.next());
            }
        };
    }

    @Override
    public String toString() {
        return Iterators.toString(iterator());
    }

    @Override
    public int hashCode() {
        return Iterators.hashCode(iterator());
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Tuple && Iterators.equals(iterator(), ((Tuple) o).iterator());
    }

}
