package com.mcintyret.rdbmstm.core.select.aggregate;

import static java.util.Arrays.asList;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Aggregators {

    private static final List<AggregatingFunction> FUNCTIONS_LIST = asList(
        new Count(),
        new Max(),
        new Min(),
        new Mean(),
        new Sum()
    );

    private static final Map<String, AggregatingFunction> FUNCTIONS = makeFunctions();

    private static Map<String, AggregatingFunction> makeFunctions() {
        Map<String, AggregatingFunction> map = new HashMap<>();
        FUNCTIONS_LIST.forEach(func -> map.put(func.getName(), func));
        return Collections.unmodifiableMap(map);
    }

    // Must be lower case.
    public static AggregatingFunction getAggregator(String name) {
        return FUNCTIONS.get(name);
    }

}
