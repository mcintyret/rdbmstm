package com.mcintyret.rdbmstm.core;

import java.util.LinkedHashMap;
import java.util.Map;

import org.testng.annotations.BeforeMethod;

import com.mcintyret.rdbmstm.query.Parser;

public class SelectTest {

    private Database database;

    private Parser parser;

    @BeforeMethod
    public void beforeMethod() {
        database = new Database("first_db");

        Map<String, ColumnDefinition> dataTypes = new LinkedHashMap<>();
        dataTypes.put("foo", new ColumnDefinition(DataType.FLOAT, false, true)); // Unique
        dataTypes.put("bar", new ColumnDefinition(DataType.INTEGER)); // Not nullable but not unique
        dataTypes.put("baz", new ColumnDefinition(DataType.STRING, true, false)); //nullable

        Table table = new Table("table_1", dataTypes);

        database.add(table);

        parser = new Parser(database);
    }


}
