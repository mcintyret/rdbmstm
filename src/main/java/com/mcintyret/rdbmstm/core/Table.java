package com.mcintyret.rdbmstm.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.mcintyret.rdbmstm.core.collect.OrderedSubsetUnmodifiableMap;
import com.sun.javafx.collections.UnmodifiableObservableMap;

public class Table {

    private final String name;

    private final ColumnDefinitions columnDefinitions = new ColumnDefinitions();

    private final Set<Tuple> rows = new HashSet<>();

    public Table(String name, Map<String, DataType> columnDefinitions) {
        this.name = name;
        this.columnDefinitions.putAll(columnDefinitions);
    }

    public void insert(List<String> colNames, List<Value> values) {
        if (colNames.size() != values.size()) {
            throw new AssertionError();
        }

        final Map<String, Value> tupleValues = new HashMap<>();
        Tuple tuple = new Tuple() {
            @Override
            public ColumnDefinitions getColumnDefinitions() {
                return columnDefinitions;
            }

            @Override
            public Map<String, Value> getValues() {
                return tupleValues;
            }
        };

        setTupleValues(colNames, values, tuple);

        if (!rows.add(tuple)) {
            throw new IllegalArgumentException("Values " + values + " cannot be inserted into columns " + colNames +
                ": duplicate row");
        }
    }

    private void setTupleValues(List<String> colNames, List<Value> values, Tuple tuple) {
        Iterator<String> colIt = colNames.iterator();
        Iterator<Value> valueIt = values.iterator();

        while (colIt.hasNext()) {
            tuple.set(colIt.next(), valueIt.next());
        }
    }

    public int update(List<String> colNames, List<Value> values, Predicate<Tuple> predicate) {
        if (colNames.size() != values.size()) {
            throw new AssertionError();
        }

        AtomicInteger count = new AtomicInteger();

        filter(predicate).forEach((tuple) -> {
            setTupleValues(colNames, values, tuple);
            count.incrementAndGet();
        });

        // TODO: check if any tuples are identical now
        return count.get();
    }

    public int delete(Predicate<Tuple> predicate) {
        // Can't do this one with streams sadly
        int count = 0;
        Iterator<Tuple> tupleIt = rows.iterator();
        while (tupleIt.hasNext()) {
            if (predicate.test(tupleIt.next())) {
                count++;
                tupleIt.remove();
            }
        }
        return count;
    }

    public Stream<Tuple> select(final List<String> colNames, Predicate<Tuple> predicate) {
        final ColumnDefinitions cols = columnDefinitions.readOnlySubset(colNames);

        return filter(predicate).map((tuple) -> {
            final Map<String, Value> values = colNames.isEmpty() ?
                tuple.getValues() :
                new OrderedSubsetUnmodifiableMap<>(tuple.getValues(), colNames);

            return new Tuple() {

                @Override
                public ColumnDefinitions getColumnDefinitions() {
                    return cols;
                }

                @Override
                public Map<String, Value> getValues() {
                    return values;
                }
            };
        });

        //TODO: ordering
    }


    private Stream<Tuple> filter(Predicate<Tuple> predicate) {
        return predicate == null ? rows.stream() : rows.stream().filter(predicate);
    }

    public String getName() {
        return name;
    }
}
