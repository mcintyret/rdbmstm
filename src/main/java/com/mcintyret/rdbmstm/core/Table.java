package com.mcintyret.rdbmstm.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.mcintyret.rdbmstm.SqlException;
import com.mcintyret.rdbmstm.collect.AliasedMap;
import com.mcintyret.rdbmstm.collect.OrderedSubsetUnmodifiableMap;

public class Table implements Relation {

    private final String name;

    private final Map<String, ColumnDefinition> columnDefinitions = new LinkedHashMap<>();

    private final Set<Tuple> rows = new HashSet<>();

    private final Map<String, Index> indices = new HashMap<>();

    public Table(String name, Map<String, ColumnDefinition> columnDefinitions) {
        this.name = name;
        this.columnDefinitions.putAll(columnDefinitions);

        for (Map.Entry<String, ColumnDefinition> entry : columnDefinitions.entrySet()) {
            if (entry.getValue().isUnique()) {
                // Create an index for all unique columns
                indices.put(entry.getKey(), new Index(entry.getKey()));
            }
        }
    }

    public void insert(Map<String, Value> values) {

        final Map<String, Value> tupleValues = new HashMap<>();
        Tuple tuple = new Tuple() {
            @Override
            public Map<String, ColumnDefinition> getColumnDefinitions() {
                return columnDefinitions;
            }

            @Override
            public Map<String, Value> getValues() {
                return tupleValues;
            }
        };

        insetTupleValues(values, tuple);

        addTuple(tuple);
    }

    private void addTuple(Tuple tuple) {
        // add to indices
        List<Index> addedIndices = new ArrayList<>(indices.size());
        try {
            for (Index index : indices.values()) {
                index.add(tuple);
                addedIndices.add(index);
            }

            if (!rows.add(tuple)) {
                throw new SqlException("Values " + tuple.getValues().values() + " cannot be inserted into columns "
                    + tuple.getValues().keySet() + ": duplicate row");
            }
        } catch (SqlException e) {
            // Rollback adding to the indices if one of them complained
            removeFromIndices(addedIndices, tuple);

            throw e;
        }
    }

    private void insetTupleValues(Map<String, Value> values, Tuple tuple) {
        for (Map.Entry<String, ColumnDefinition> entry : columnDefinitions.entrySet()) {
            String colName = entry.getKey();
            ColumnDefinition colDef = entry.getValue();

            Value val = values.get(colName);
            if (val == null) {
                val = colDef.getDefaultValue();
            }

            if (val.isNull() && !colDef.isNullable()) {
                throw new SqlException("Cannot insert NULL value on non-NULLABLE column '" + colName + "'");
            }

            tuple.set(colName, val);
        }
    }

    private static void removeFromIndices(Iterable<Index> indices, Tuple tuple) {
        for (Index index : indices) {
            index.remove(tuple);
        }
    }

    private void removeFromIndices(Tuple tuple) {
        removeFromIndices(indices.values(), tuple);
    }

    // Updates are expensive because we respect proper relational stuff
    // - no two rows can be the same!
    public int update(Map<String, Value> updates, Predicate<Tuple> predicate) {
        Collection<Tuple> removed = new ArrayList<>();
        Iterator<Tuple> it = rows.iterator();
        while (it.hasNext()) {
            Tuple tuple = it.next();
            if (predicate.test(tuple)) {
                removed.add(tuple);
                removeFromIndices(tuple);
                it.remove();
            }
        }

        List<Tuple> tuplesBeforeUpdate = new ArrayList<>(removed.size());
        List<Tuple> tuplesAfterUpdate = new ArrayList<>(removed.size());
        try {
            for (Tuple tuple : removed) {
                tuplesBeforeUpdate.add(tuple.copy());

                // now update the original tuple
                for (Map.Entry<String, Value> entry : updates.entrySet()) {
                    ColumnDefinition cd = columnDefinitions.get(entry.getKey());

                    if (entry.getValue().isNull() && !cd.isNullable()) {
                        throw new SqlException("Cannot update NULL value on non-NULLABLE column '" + entry.getKey() + "'");
                    }

                    tuple.getValues().put(entry.getKey(), entry.getValue());
                }

                addTuple(tuple);
                tuplesAfterUpdate.add(tuple);
            }
        } catch (SqlException e) {
            // rollback.
            rows.removeAll(tuplesAfterUpdate);
            rows.addAll(tuplesBeforeUpdate);

            throw e;
        }

        return removed.size();
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
        final Map<String, ColumnDefinition> cols = colAliases.isEmpty() ?
            getColumnDefinitions() :
            new AliasedMap<>(colAliases, columnDefinitions);

        Stream<Tuple> rows = filter(predicate).map((tuple) -> {
            final Map<String, Value> values = new OrderedSubsetUnmodifiableMap<>(tuple.getValues(), cols.keySet());

            return new Tuple() {

                @Override
                public Map<String, ColumnDefinition> getColumnDefinitions() {
                    return cols;
                }

                @Override
                public Map<String, Value> getValues() {
                    return values;
                }
            };
        }).distinct();

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
            public Map<String, ColumnDefinition> getColumnDefinitions() {
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
    public Map<String, ColumnDefinition> getColumnDefinitions() {
        return Collections.unmodifiableMap(columnDefinitions);
    }
}
