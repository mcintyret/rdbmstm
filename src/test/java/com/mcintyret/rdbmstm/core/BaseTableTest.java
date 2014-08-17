package com.mcintyret.rdbmstm.core;

import java.util.LinkedHashMap;
import java.util.Map;

import org.testng.annotations.BeforeMethod;

import com.mcintyret.rdbmstm.query.Parser;

public class BaseTableTest {

    protected Database database;

    protected Parser parser;

    protected Map<String, ColumnDefinition> colDefs;

    @BeforeMethod
    public void beforeMethod() {
        database = new Database("first_db");

        colDefs = new LinkedHashMap<>();
        colDefs.put("foo", new ColumnDefinition(DataType.FLOAT, false, true)); // Unique
        colDefs.put("bar", new ColumnDefinition(DataType.INTEGER)); // Not nullable but not unique
        colDefs.put("baz", new ColumnDefinition(DataType.STRING, true, false)); //nullable

        Table table = new Table("table_1", colDefs);

        database.add(table);

        parser = new Parser(database);

        parser.parse("insert into table_1 values (15.46, 17, 'testing');").executeModification();
        parser.parse("insert into table_1 values (13, 27, 'foo');").executeModification();
        parser.parse("insert into table_1 values (57735.12, 1, 'this');").executeModification();
        parser.parse("insert into table_1 values (0.0007, 3486978, 'is');").executeModification();
        parser.parse("insert into table_1 values (1556.345, 8975, 'testing');").executeModification();

    }

}
