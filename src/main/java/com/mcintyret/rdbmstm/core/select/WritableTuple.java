package com.mcintyret.rdbmstm.core.select;

import com.mcintyret.rdbmstm.core.Tuple;
import com.mcintyret.rdbmstm.core.Value;

public interface WritableTuple extends Tuple {

    void set(String colName, Value value);

}
