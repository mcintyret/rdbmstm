package com.mcintyret.rdbmstm.core.select.aggregate;

import java.util.stream.DoubleStream;

public class Min extends NumericalAggregatingFunction {

    @Override
    public String getName() {
        return "min";
    }

    @Override
    protected double toValue(DoubleStream doubleStream) {
        return doubleStream.min().getAsDouble();
    }
}
