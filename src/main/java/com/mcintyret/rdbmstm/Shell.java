package com.mcintyret.rdbmstm;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;

import com.mcintyret.rdbmstm.core.DataType;
import com.mcintyret.rdbmstm.core.Database;
import com.mcintyret.rdbmstm.core.Table;
import com.mcintyret.rdbmstm.query.Parser;

public class Shell {

    public static void main(String[] args) {
        Database database = new Database("first_db");

        Map<String, DataType> dataTypes = new LinkedHashMap<>();
        dataTypes.put("foo", DataType.FLOAT);
        dataTypes.put("bar", DataType.INTEGER);
        dataTypes.put("baz", DataType.STRING);


        Table table = new Table("table_1", dataTypes);

        database.add(table);

        Parser parser = new Parser(database);

        Scanner scanner = new Scanner(System.in);


        while (true) {
            System.out.println(">");
            String query = scanner.nextLine();

            try {
                parser.parse(query).execute(database);
            } catch (SqlException e) {
                e.printStackTrace();
            }

        }
    }

}
