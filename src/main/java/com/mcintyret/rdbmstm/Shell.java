package com.mcintyret.rdbmstm;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;

import com.mcintyret.rdbmstm.core.ColumnDefinition;
import com.mcintyret.rdbmstm.core.DataType;
import com.mcintyret.rdbmstm.core.Database;
import com.mcintyret.rdbmstm.core.Table;
import com.mcintyret.rdbmstm.query.Parser;

public class Shell {

    public static void main(String[] args) {
        Database database = new Database("first_db");

        Map<String, ColumnDefinition> dataTypes = new LinkedHashMap<>();
        dataTypes.put("foo", new ColumnDefinition(DataType.FLOAT));
        dataTypes.put("bar", new ColumnDefinition(DataType.INTEGER));
        dataTypes.put("baz", new ColumnDefinition(DataType.STRING));

        Table table = new Table("table_1", dataTypes);

        database.add(table);

        Parser parser = new Parser(database);

        Scanner scanner = new Scanner(System.in);


        while (true) {
            StringBuilder sb = new StringBuilder();
            do {
                System.out.print("> ");
                sb.append(scanner.nextLine());
            } while (sb.length() == 0 || sb.charAt(sb.length() - 1) != ';');

            try {
                parser.parse(sb.toString()).execute(database);
            } catch (SqlException e) {
                e.printStackTrace();
            }

        }
    }

}
