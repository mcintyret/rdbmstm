package com.mcintyret.rdbmstm.core.select;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.mcintyret.rdbmstm.collect.AliasedMap;
import com.mcintyret.rdbmstm.core.AbstractTuple;
import com.mcintyret.rdbmstm.core.ColumnDefinition;
import com.mcintyret.rdbmstm.core.Relation;
import com.mcintyret.rdbmstm.core.Tuple;
import com.mcintyret.rdbmstm.core.Value;

public class AliasingSelector implements Selector {

    private final Map<String, String> aliases;

    public AliasingSelector(Map<String, String> aliases) {
        this.aliases = Collections.unmodifiableMap(aliases);
    }

    @Override
    public Relation select(Relation in) {
        final Map<String, String> allAliases = new HashMap<>(aliases);

        in.getColumnNames().forEach(name -> allAliases.put(name, name));

        final Map<String, ColumnDefinition> cols = new AliasedMap<>(allAliases, in.getColumnDefinitions());

        return new Relation() {
            @Override
            public Stream<? extends Tuple> getValues() {
                return in.getValues().map(tuple -> {
                    return new AbstractTuple() {

                        @Override
                        protected Value doSelect(String colName) {
                            return tuple.select(allAliases.get(colName));
                        }

                        @Override
                        public Map<String, ColumnDefinition> getColumnDefinitions() {
                            return cols;
                        }
                    };
                });
            }

            @Override
            public Map<String, ColumnDefinition> getColumnDefinitions() {
                return cols;
            }
        };
    }
}
