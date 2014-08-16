package com.mcintyret.rdbmstm.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableTestUtils {

    public static Relation toRelation(Map<String, ColumnDefinition> cols, Object[][] table) {
        return new Relation() {
            @Override
            public String getName() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Collection<String> getColumnNames() {
                return cols.keySet();
            }

            @Override
            public Stream<? extends Collection<Value>> getValues() {
                return toList(table).stream();
            }

            @Override
            public Map<String, ColumnDefinition> getColumnDefinitions() {
                return cols;
            }
        };
    }

    private static List<? extends Collection<Value>> toList(Object[][] table) {
        List<Collection<Value>> list = new ArrayList<>(table.length);
        for (Object[] aTable : table) {
            List<Value> row = new ArrayList<>(aTable.length);
            for (Object val : aTable) {
                if (val instanceof DataType) {
                    row.add(Value.nullOf((DataType) val));
                } else if (val instanceof Float || val instanceof Double) {
                    row.add(Value.of(((Number) val).doubleValue()));
                } else if (val instanceof Integer || val instanceof Long) {
                    row.add(Value.of(((Number) val).longValue()));
                } else if (val instanceof String) {
                    row.add(Value.of(val.toString()));
                } else {
                    throw new IllegalArgumentException("Unsupported value type: " + val.getClass());
                }
            }
            list.add(row);
        }
        return list;
    }

    public static void assertRelationEquals(Relation actual, Relation expected) {
        // Don't care about name so much
        assertOrderedMaps(actual.getColumnDefinitions(), expected.getColumnDefinitions());

        List<? extends Collection<Value>> actualValues =
            actual.getValues().collect(Collectors.<Collection<Value>>toList());

        List<? extends Collection<Value>> expectedValues =
            expected.getValues().collect(Collectors.<Collection<Value>>toList());

        assertIterablesEqual(actualValues, expectedValues);
    }


    private static void assertOrderedMaps(Map<?, ?> actual, Map<?, ?> expected) {
        assertIterablesEqual(actual.entrySet(), expected.entrySet());
    }

    private static void assertIterablesEqual(Iterable<?> actual, Iterable<?> expected) {
        assertIteratorsEqual(actual.iterator(), expected.iterator());
    }

    private static void assertIteratorsEqual(Iterator<?> actual, Iterator<?> expected) {
        int count = 0;
        while (actual.hasNext()) {
            count++;
            if (!expected.hasNext()) {
                throw new AssertionError("Actual has more elements than expected - at least " + count);
            }

            Object actObj = actual.next();
            Object expObj = expected.next();

            if (!Objects.equals(actObj, expObj)) {
                throw new AssertionError("Expected [" + expObj + "] at position " + count +
                    " but found [" + actObj + "]");
            }
        }

        if (expected.hasNext()) {
            throw new AssertionError("Actual has fewer elements than expected - only " + count);
        }

    }
}
