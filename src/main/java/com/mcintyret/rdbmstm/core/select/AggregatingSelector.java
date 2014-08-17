package com.mcintyret.rdbmstm.core.select;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.mcintyret.rdbmstm.core.AbstractTuple;
import com.mcintyret.rdbmstm.core.ColumnDefinition;
import com.mcintyret.rdbmstm.core.Relation;
import com.mcintyret.rdbmstm.core.Tuple;
import com.mcintyret.rdbmstm.core.Value;
import com.mcintyret.rdbmstm.core.select.aggregate.AggregatingFunction;

public class AggregatingSelector implements Selector {

    private final Map<String, AggregatingFunction> functions;

    public AggregatingSelector(Map<String, AggregatingFunction> functions) {
        this.functions = functions;
    }

    @Override
    public Relation select(Relation in) {
        Tuple sample = in.getValues().findFirst().get();
        Map<String, ColumnDefinition> colDefs = new LinkedHashMap<>(in.getColumnDefinitions());

        // TODO: basically doing a copy here. Is there a nicer way?
        Map<String, Value> aggregatedValues = new HashMap<>();
        sample.getColumnNames().forEach(colName -> {
            aggregatedValues.put(colName, sample.select(colName));
        });

        // Overwrite with the aggregate values
        functions.forEach((colName, function) -> {
            Value aggregatedVal = function.aggregate(colName, in.getValues());
            aggregatedValues.put(colName, aggregatedVal);
            colDefs.put(colName, function.getColumnDefinition());
        });

        Tuple theTuple = new AbstractTuple() {

            @Override
            protected Value doSelect(String colName) {
                return aggregatedValues.get(colName);
            }

            @Override
            public Map<String, ColumnDefinition> getColumnDefinitions() {
                return colDefs;
            }
        };

        return new Relation() {
            @Override
            public Stream<? extends Tuple> getValues() {
                return Stream.of(theTuple);
            }

            @Override
            public Map<String, ColumnDefinition> getColumnDefinitions() {
                return colDefs;
            }
        };
    }
}
