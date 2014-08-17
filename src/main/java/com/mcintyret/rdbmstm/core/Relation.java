package com.mcintyret.rdbmstm.core;

import java.util.stream.Stream;

public interface Relation extends Columnar {

    public Stream<? extends Tuple> getValues();

}
