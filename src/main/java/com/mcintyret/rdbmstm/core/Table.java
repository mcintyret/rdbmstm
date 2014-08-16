package com.mcintyret.rdbmstm.core;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.mcintyret.rdbmstm.SqlException;
import com.mcintyret.rdbmstm.collect.AliasedMap;
import com.mcintyret.rdbmstm.collect.OrderedSubsetUnmodifiableMap;

public class Table implements Relation {

    private final String name;

    private final ColumnDefinitions columnDefinitions = new ColumnDefinitions();

    private final Set<Tuple> rows = new HashSet<>();

    public Table(String name, Map<String, DataType> columnDefinitions) {
        this.name = name;
        this.columnDefinitions.putAll(columnDefinitions);
    }

    public void insert(Collection<String> colNames, Collection<Value> values) {
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
            throw new SqlException("Values " + values + " cannot be inserted into columns " + colNames + ": duplicate row");
        }
    }

    private void setTupleValues(Collection<String> colNames, Collection<Value> values, Tuple tuple) {
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

    public Relation select(Map<String, String> colAliases, Predicate<Tuple> predicate) {
        final Map<String, DataType> cols = colAliases.isEmpty() ?
            getColumnDefinitions() :
            new AliasedMap<>(colAliases, columnDefinitions);

        Stream <Tuple> rows =  filter(predicate).map((tuple) -> {
            final Map<String, Value> values = new OrderedSubsetUnmodifiableMap<>(tuple.getValues(), cols.keySet());

            return new Tuple() {

                @Override
                public Map<String, DataType> getColumnDefinitions() {
                    return cols;
                }

                @Override
                public Map<String, Value> getValues() {
                    return values;
                }
            };
        });

        //TODO: ordering

        return new Relation() {
            @Override
            public String getName() {
                return "Selection";
            }

            @Override
            public Collection<String> getColumnNames() {
                return cols.keySet();
            }

            @Override
            public Stream<? extends Collection<Value>> getValues() {
                return rows;
            }

            @Override
            public Map<String, DataType> getColumnDefinitions() {
                return cols;
            }
        };
    }


    private Stream<Tuple> filter(Predicate<Tuple> predicate) {
        return predicate == null ? rows.stream() : rows.stream().filter(predicate);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Collection<String> getColumnNames() {
        return columnDefinitions.keySet();
    }

    @Override
    public Stream<? extends Collection<Value>> getValues() {
        return rows.stream();
    }

    @Override
    public Map<String, DataType> getColumnDefinitions() {
        return Collections.unmodifiableMap(columnDefinitions);
    }
}
