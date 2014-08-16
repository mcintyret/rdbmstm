package com.mcintyret.rdbmstm.core;

import java.util.Collection;
import java.util.Map;

public interface Relation {

    String getName();

    Collection<String> getColumnNames();

    public Collection<? extends Collection<Value>> getValues();

    Map<String, DataType> getColumnDefinitions();

}
