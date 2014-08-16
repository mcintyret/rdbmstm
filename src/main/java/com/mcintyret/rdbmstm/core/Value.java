package com.mcintyret.rdbmstm.core;

public class Value {

    private final DataType dataType;

    private final Object value;

    private Value(DataType dataType, Object value) {
        this.dataType = dataType;
        this.value = value;
    }

    public static Value of(final String string) {
        return new Value(DataType.STRING, string);
    }

    public static Value of(final long l) {
        return new Value(DataType.INTEGER, l);
    }

    public static Value of(final double d) {
        return new Value(DataType.FLOAT, d);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Value value1 = (Value) o;

        if (dataType != value1.dataType) return false;
        if (value != null ? !value.equals(value1.value) : value1.value != null) return false;

        return true;
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

    @Override
    public String toString() {
        return String.valueOf(value);
    }


    public Object getValue() {
        return value;
    }
}
