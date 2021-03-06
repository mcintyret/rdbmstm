package com.mcintyret.rdbmstm.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableTestUtils {

    public static Relation toRelation(Map<String, ColumnDefinition> cols, Object[][] table) {
        return new Relation() {
            @Override
            public Collection<String> getColumnNames() {
                return cols.keySet();
            }

            @Override
            public Stream<? extends Tuple> getValues() {
                return toList(table).stream();
            }

            @Override
            public Map<String, ColumnDefinition> getColumnDefinitions() {
                return cols;
            }
        };
    }

    private static List<? extends Tuple> toList(Object[][] table) {
        List<Tuple> list = new ArrayList<>(table.length);
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
            list.add(new SimpleListTuple(row));
        }
        return list;
    }

    public static void assertRelationsEqual(Relation actual, Relation expected) {
        assertOrderedMapsEqual(actual.getColumnDefinitions(), expected.getColumnDefinitions());

        Set<? extends Iterable<Value>> actualValues =
            actual.getValues().collect(Collectors.<Iterable<Value>>toSet());

        Set<? extends Iterable<Value>> expectedValues =
            expected.getValues().collect(Collectors.<Iterable<Value>>toSet());

        assertUnorderedSetsEqual(actualValues, expectedValues);
    }

    public static void assertOrderedRelationsEqual(Relation actual, Relation expected) {
        assertOrderedMapsEqual(actual.getColumnDefinitions(), expected.getColumnDefinitions());

        List<? extends Iterable<Value>> actualValues =
            actual.getValues().collect(Collectors.<Iterable<Value>>toList());

        List<? extends Iterable<Value>> expectedValues =
            expected.getValues().collect(Collectors.<Iterable<Value>>toList());

        assertIterablesEqual(actualValues, expectedValues);
    }


    public static void assertOrderedMapsEqual(Map<?, ?> actual, Map<?, ?> expected) {
        assertIterablesEqual(actual.entrySet(), expected.entrySet());
    }

    public static void assertIterablesEqual(Iterable<?> actual, Iterable<?> expected) {
        assertIteratorsEqual(actual.iterator(), expected.iterator());
    }

    public static void assertUnorderedSetsEqual(Set<?> actual, Set<?> expected) {
        Set<?> inActualButNotExpected = new HashSet<>(actual);
        inActualButNotExpected.removeAll(expected);

        Set<?> inExpectedButNotActual = new HashSet<>(expected);
        inExpectedButNotActual.removeAll(actual);

        StringBuilder sb = new StringBuilder();
        if (!inActualButNotExpected.isEmpty()) {
            sb.append("In actual but not expected: \n");
            inActualButNotExpected.forEach((o) -> sb.append(o).append("\n"));
        }
        if (!inExpectedButNotActual.isEmpty()) {
            sb.append("In expected but not actual: \n");
            inExpectedButNotActual.forEach((o) -> sb.append(o).append("\n"));
        }

        if (sb.length() != 0) {
            throw new AssertionError(sb.toString());
        }
    }

    public static void assertIteratorsEqual(Iterator<?> actual, Iterator<?> expected) {
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

    private static class SimpleListTuple extends AbstractTuple {

        private final List<Value> list;

        private SimpleListTuple(List<Value> list) {
            this.list = list;
        }

        @Override
        public Iterator<Value> iterator() {
            return list.iterator();
        }

        @Override
        protected Value doSelect(String colName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, ColumnDefinition> getColumnDefinitions() {
            throw new UnsupportedOperationException();
        }
    }
}
