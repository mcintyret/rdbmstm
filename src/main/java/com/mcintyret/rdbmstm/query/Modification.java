package com.mcintyret.rdbmstm.query;

public class Modification {

    enum UpdateType {INSERT, UPDATE, DELETE}

    private final UpdateType type;

    private final int num;

    private static final Modification INSERT = new Modification(UpdateType.INSERT, 1);

    public static Modification insert() {
        return INSERT;
    }

    public static Modification delete(int num) {
        return new Modification(UpdateType.DELETE, num);
    }

    public static Modification update(int num) {
        return new Modification(UpdateType.UPDATE, num);
    }

    private Modification(UpdateType type, int num) {
        this.type = type;
        this.num = num;
    }

    public UpdateType getType() {
        return type;
    }

    public int getNum() {
        return num;
    }
}
