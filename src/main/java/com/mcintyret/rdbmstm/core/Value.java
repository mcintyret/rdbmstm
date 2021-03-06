package com.mcintyret.rdbmstm.core;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import com.mcintyret.rdbmstm.SqlException;

public class Value implements Comparable<Value> {

    private final DataType dataType;

    private final Comparable value;

    private static final Map<DataType, Value> NULL_VALUE_CACHE = makeNullValues();

    private static Map<DataType, Value> makeNullValues() {
        Map<DataType, Value> map = new EnumMap<>(DataType.class);
        for (DataType dt : DataType.values()) {
            map.put(dt, new Value(dt, null));
        }
        return Collections.unmodifiableMap(map);
    }

    private Value(DataType dataType, Comparable value) {
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

    public static Value parse(String val, DataType dataType) {
        if ("null".equals(val)) {
            return Value.nullOf(dataType);
        }
        try {
            switch (dataType) {
                case STRING:
                    if (!val.startsWith("'") || !val.endsWith("'")) {
                        throw new SqlException("Illegal string format: <" + val + "> must be wrapped in single quotes");
                    }
                    return Value.of(val.substring(1, val.length() - 1));
                case INTEGER:
                    return Value.of(Long.parseLong(val));
                case FLOAT:
                    return Value.of(Double.parseDouble(val));
                default:
                    throw new SqlException("Unsupported DataType: " + dataType);
            }
        } catch (NumberFormatException e) {
            throw new SqlException("Illegal number format for DataType " + dataType + ": '" + val + "'", e);
        }

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


    public Comparable getValue() {
        return value;
    }

    @Override
    public int compareTo(Value o) {
        return value.compareTo(o.value);
    }
}
