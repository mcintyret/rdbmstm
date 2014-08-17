package com.mcintyret.rdbmstm.core.select.aggregate;

import java.util.stream.DoubleStream;

public class Max extends NumericalAggregatingFunction {

    @Override
    public String getName() {
        return "max";
    }

    @Override
    protected double toValue(DoubleStream doubleStream) {
        return doubleStream.max().getAsDouble();
    }
}
