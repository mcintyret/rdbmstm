package com.mcintyret.rdbmstm.core.select;

import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.mcintyret.rdbmstm.core.ColumnDefinition;
import com.mcintyret.rdbmstm.core.Relation;
import com.mcintyret.rdbmstm.core.Tuple;

public class FilteringSelector implements Selector {

    private final Predicate<Tuple> predicate;

    public FilteringSelector(Predicate<Tuple> predicate) {
        this.predicate = predicate;
    }

    @Override
    public Relation select(Relation in) {
        return new Relation() {
            @Override
            public Stream<? extends Tuple> getValues() {
                return in.getValues().filter(predicate);
            }

            @Override
            public Map<String, ColumnDefinition> getColumnDefinitions() {
                return in.getColumnDefinitions();
            }
        };
    }

}
