package com.mcintyret.rdbmstm.core;

public class Value {

    private final DataType dataType;

    private final Object value;

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
        return new Value(dataType, null);
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

    @Override
    public String toString() {
        return String.valueOf(value);
    }


    public Object getValue() {
        return value;
    }
}
