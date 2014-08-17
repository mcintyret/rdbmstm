package com.mcintyret.rdbmstm.core.select.aggregate;

import java.util.stream.DoubleStream;

public class Sum extends NumericalAggregatingFunction {

    @Override
    public String getName() {
        return "sum";
    }

    @Override
    protected double toValue(DoubleStream doubleStream) {
        return doubleStream.sum();
    }
}
