package com.mcintyret.rdbmstm.core.select;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

import com.mcintyret.rdbmstm.core.ColumnDefinition;
import com.mcintyret.rdbmstm.core.Relation;
import com.mcintyret.rdbmstm.core.Tuple;

public class OrderingSelector implements Selector {

    private final Comparator<Tuple> comparator;

    public OrderingSelector(Comparator<Tuple> comparator) {
        this.comparator = comparator;
    }

    @Override
    public Relation select(Relation in) {

        return new Relation() {
            @Override
            public Stream<? extends Tuple> getValues() {
                return in.getValues().sorted(comparator);
            }

            @Override
            public Map<String, ColumnDefinition> getColumnDefinitions() {
                return in.getColumnDefinitions();
            }
        };
    }
}
