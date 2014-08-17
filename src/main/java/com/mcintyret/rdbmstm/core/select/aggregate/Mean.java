package com.mcintyret.rdbmstm.core.select.aggregate;

import java.util.stream.DoubleStream;

public class Mean extends NumericalAggregatingFunction {

    @Override
    public String getName() {
        return "mean";
    }

    @Override
    protected double toValue(DoubleStream doubleStream) {
        return doubleStream.average().getAsDouble();
    }
}
