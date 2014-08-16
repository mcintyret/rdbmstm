package com.mcintyret.rdbmstm.core;

import java.util.Iterator;
import java.util.Map;

public interface Tuple extends Iterable<Value>, Columnar {

    Value select(String colName);

    void set(String colName, Value value);

}
