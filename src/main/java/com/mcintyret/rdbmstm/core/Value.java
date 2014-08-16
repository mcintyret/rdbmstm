package com.mcintyret.rdbmstm.core;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

public class Value {

    private final DataType dataType;

    private final Object value;

    private static final Map<DataType, Value> NULL_VALUE_CACHE = makeNullValues();

    private static Map<DataType, Value> makeNullValues() {
        Map<DataType, Value> map = new EnumMap<>(DataType.class);
        for (DataType dt : DataType.values()) {
            map.put(dt, new Value(dt, null));
        }
        return Collections.unmodifiableMap(map);
    }

    private Value(DataType dataType, Object value) {
        this.dataType = dataType;
        this.value = value;
    }

    public static Value of(String string) {
        return new Value(DataType.STRING, string);
    }

    public static Value of(long l) {
        return new Value(DataType.INTEGER, l);
    }

    public static Value of(double d) {
        return new Value(DataType.FLOAT, d);
    }

    public static Value nullOf(DataType dataType) {
        return NULL_VALUE_CACHE.get(dataType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Value value1 = (Value) o;

        return dataType == value1.dataType &&
            !(value != null ? !value.equals(value1.value) : value1.value != null);

    }

    @Override
    public int hashCode() {
        int result = dataType.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean isNull() {
        return value == null;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }


    public Object getValue() {
        return value;
    }
}
