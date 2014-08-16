package com.mcintyret.rdbmstm.core;

public class Name {

    private final String realName;

    private final String alias;

    public Name(String realName) {
        this(realName, realName);
    }

    public Name(String realName, String alias) {
        this.realName = realName;
        this.alias = alias;
    }
}
