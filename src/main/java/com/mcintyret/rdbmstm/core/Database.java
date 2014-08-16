package com.mcintyret.rdbmstm.core;

import java.util.HashMap;
import java.util.Map;

import com.mcintyret.rdbmstm.SqlException;

public class Database {

    private final String name;

    private final Map<String, Table> tables = new HashMap<>();

    public Database(String name) {
        this.name = name;
    }

    public void add(Table table) {
        if (tables.putIfAbsent(table.getName(), table) != null) {
            throw new SqlException("Cannot create table with name '" + table.getName()
            + "' - table already exists");
        }
    }

    public void drop(String name) {
        if (tables.remove(name) == null) {
            throw new SqlException("No such table: " + name);
        }
    }

    public Table get(String name) {
        Table table = tables.get(name);
        if (table == null) {
            throw new SqlException("No such table: " + name);
        }
        return table;
    }

    public String getName() {
        return name;
    }
}
