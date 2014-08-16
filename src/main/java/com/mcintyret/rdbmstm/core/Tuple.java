package com.mcintyret.rdbmstm.core;

public interface Tuple extends Iterable<Value>, Columnar {

    Value select(String colName);

    void set(String colName, Value value);

}
