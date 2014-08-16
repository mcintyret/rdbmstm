package com.mcintyret.rdbmstm.core;

import java.util.stream.Stream;

public interface Relation extends Columnar {

    String getName();

    public Stream<? extends Tuple> getValues();


}
