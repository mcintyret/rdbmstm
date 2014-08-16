package com.mcintyret.rdbmstm.core;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

public interface Relation {

    String getName();

    Collection<String> getColumnNames();

    public Stream<? extends Collection<Value>> getValues();

    Map<String, DataType> getColumnDefinitions();

}
