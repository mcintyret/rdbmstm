package com.mcintyret.rdbmstm.core;

import java.util.Collection;
import java.util.Map;

public interface Columnar {

    default Collection<String> getColumnNames() {
        return getColumnDefinitions().keySet();
    }

    Map<String, ColumnDefinition> getColumnDefinitions();
}
