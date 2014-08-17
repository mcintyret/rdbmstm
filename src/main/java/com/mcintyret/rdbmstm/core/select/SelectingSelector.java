package com.mcintyret.rdbmstm.core.select;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.mcintyret.rdbmstm.collect.OrderedSubsetUnmodifiableMap;
import com.mcintyret.rdbmstm.core.AbstractTuple;
import com.mcintyret.rdbmstm.core.ColumnDefinition;
import com.mcintyret.rdbmstm.core.Relation;
import com.mcintyret.rdbmstm.core.Tuple;
import com.mcintyret.rdbmstm.core.Value;

public class SelectingSelector implements Selector {

    private final Set<String> orderedColumns;

    public SelectingSelector(Set<String> orderedColumns) {
        this.orderedColumns = orderedColumns;
    }

    @Override
    public Relation select(Relation in) {
        final Map<String, ColumnDefinition> cols = new OrderedSubsetUnmodifiableMap<>(in.getColumnDefinitions(), orderedColumns);

        return new Relation() {
            @Override
            public Stream<? extends Tuple> getValues() {
                return in.getValues().map(tuple -> {
                    return new AbstractTuple() {

                        @Override
                        protected Value doSelect(String colName) {
                            return tuple.select(colName);
                        }

                        @Override
                        public Map<String, ColumnDefinition> getColumnDefinitions() {
                            return cols;
                        }
                    };
                }).distinct();
            }

            @Override
            public Map<String, ColumnDefinition> getColumnDefinitions() {
                return cols;
            }
        };
    }
}
