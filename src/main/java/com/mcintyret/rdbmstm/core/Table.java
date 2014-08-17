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
import com.mcintyret.rdbmstm.core.select.Selector;

public class Table implements NamedRelation {

    private final String name;

    private final Map<String, ColumnDefinition> columnDefinitions = new LinkedHashMap<>();

    private final Set<Row> rows = new HashSet<>();

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

        Row tuple = new Row() {
            @Override
            public Map<String, ColumnDefinition> getColumnDefinitions() {
                return columnDefinitions;
            }
        };

        insetTupleValues(values, tuple);

        addRow(tuple);
    }

    private void addRow(Row tuple) {
        // add to indices
        List<Index> addedIndices = new ArrayList<>(indices.size());
        try {
            for (Index index : indices.values()) {
                index.add(tuple);
                addedIndices.add(index);
            }

            if (!rows.add(tuple)) {
                throw new SqlException("Values " + tuple + " cannot be inserted into columns "
                    + tuple.getColumnNames() + ": duplicate row");
            }
        } catch (SqlException e) {
            // Rollback adding to the indices if one of them complained
            removeFromIndices(addedIndices, tuple);

            throw e;
        }
    }

    private void insetTupleValues(Map<String, Value> values, Row tuple) {
        for (Map.Entry<String, ColumnDefinition> entry : columnDefinitions.entrySet()) {
            String colName = entry.getKey();
            ColumnDefinition colDef = entry.getValue();

            Value val = values.get(colName);
            if (val == null) {
                val = colDef.getDefaultValue();
            }

            tuple.set(colName, val);
        }
    }

    private static void removeFromIndices(Iterable<Index> indices, Row tuple) {
        for (Index index : indices) {
            index.remove(tuple);
        }
    }

    private void removeFromIndices(Row tuple) {
        removeFromIndices(indices.values(), tuple);
    }

    // Updates are expensive because we respect proper relational stuff
    // - no two rows can be the same!
    public int update(Map<String, Value> updates, Predicate<Tuple> predicate) {
        Collection<Row> removed = new ArrayList<>();
        Iterator<Row> it = rows.iterator();
        while (it.hasNext()) {
            Row row = it.next();
            if (predicate.test(row)) {
                removed.add(row);
                removeFromIndices(row);
                it.remove();
            }
        }

        List<Row> rowsBeforeUpdate = new ArrayList<>(removed.size());
        List<Row> rowsAfterUpdate = new ArrayList<>(removed.size());
        try {
            Iterator<Row> removedIt = removed.iterator();
            while (removedIt.hasNext()) {
                Row row = removedIt.next();
                rowsBeforeUpdate.add(row.copy());
                removedIt.remove();

                // now update the original row
                for (Map.Entry<String, Value> entry : updates.entrySet()) {
                    row.set(entry.getKey(), entry.getValue());
                }

                addRow(row);
                rowsAfterUpdate.add(row);
            }
        } catch (SqlException e) {
            // rollback.
            rows.removeAll(rowsAfterUpdate);
            for (Row row : rowsAfterUpdate) {
                removeFromIndices(row);
            }

            for (Row row : rowsBeforeUpdate) {
                addRow(row);
            }
            for (Row row : removed) {
                addRow(row);
            }

            throw e;
        }

        return removed.size();
    }

    public int delete(Predicate<Tuple> predicate) {
        // Can't do this one with streams sadly
        int count = 0;
        Iterator<Row> tupleIt = rows.iterator();
        while (tupleIt.hasNext()) {
            if (predicate.test(tupleIt.next())) {
                count++;
                tupleIt.remove();
            }
        }
        return count;
    }

    public Relation select(Selector selector) {
        return selector.select(this);
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
    public Stream<Row> getValues() {
        return rows.stream();
    }

    @Override
    public Map<String, ColumnDefinition> getColumnDefinitions() {
        return Collections.unmodifiableMap(columnDefinitions);
    }
}
