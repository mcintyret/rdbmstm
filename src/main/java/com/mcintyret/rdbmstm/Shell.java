package com.mcintyret.rdbmstm;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;

import com.mcintyret.rdbmstm.core.ColumnDefinition;
import com.mcintyret.rdbmstm.core.DataType;
import com.mcintyret.rdbmstm.core.Database;
import com.mcintyret.rdbmstm.core.Table;
import com.mcintyret.rdbmstm.query.Execution;
import com.mcintyret.rdbmstm.query.Modification;
import com.mcintyret.rdbmstm.query.Parser;

public class Shell {

    public static void main(String[] args) {
        Database database = new Database("first_db");

        Map<String, ColumnDefinition> dataTypes = new LinkedHashMap<>();
        dataTypes.put("foo", new ColumnDefinition(DataType.FLOAT, false, true)); // Unique
        dataTypes.put("bar", new ColumnDefinition(DataType.INTEGER)); // Not nullable but not unique
        dataTypes.put("baz", new ColumnDefinition(DataType.STRING, true, false)); //nullable

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
                Execution ex = parser.parse(sb.toString());
                if (ex.isQuery()) {
                    System.out.println(Formatter.toString(ex.executeQuery()));
                } else {
                    Modification mod = ex.executeModification();
                    System.out.println("Successfully executed " + mod.getType() + " on " + mod.getNum() + " rows");
                }

            } catch (SqlException e) {
                e.printStackTrace();
            }

        }
    }

}
