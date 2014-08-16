package com.mcintyret.rdbmstm.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import com.mcintyret.rdbmstm.core.DataType;
import com.mcintyret.rdbmstm.core.Database;
import com.mcintyret.rdbmstm.core.Relation;
import com.mcintyret.rdbmstm.core.Table;
import com.mcintyret.rdbmstm.core.Tuple;
import com.mcintyret.rdbmstm.core.Value;

public class Parser {

    private final Database database;

    public Parser(Database database) {
        this.database = database;
    }

    public Query parse(String sql) throws SqlParseException {
        Iterator<String> parts = Arrays.asList(preProcess(sql).split("\\s+")).iterator();
        try {
            switch (parts.next()) {
                case "create":
//                    return parseCreate(parts);
                    break;
                case "select":
                    return parseSelect(parts);
                case "update":
//                    return parseUpdate(parts);
                    break;
                case "delete":
//                    return parseDelete(parts);
                    break;
                case "insert":
                    return parseInsert(parts);
                default:
                    throw new SqlParseException("Cannot parse query: " + sql);
            }
        } catch (SqlParseException e) {
            throw new SqlParseException("Error parsing sql '" + sql + "': " + e.getMessage(), e);
        }

        throw new SqlParseException("Not handled yet: " + sql);
    }

    private Query parseInsert(Iterator<String> parts) throws SqlParseException {
        assertNextToken("into", parts);

        String tableName = parts.next();
        Table table = database.get(tableName);

        Collection<String> columnNames = new ArrayList<>();
        String name;
        while (parts.hasNext() && !"values".equals((name = parts.next()))) {
            columnNames.add(name);
        }

        if (columnNames.isEmpty()) {
            columnNames = table.getColumnNames();
        }

        assertNextToken("(", parts);

        List<Value> values = parseValues(parts, columnNames, table);

        final Collection<String> finColNames = columnNames;
        return database -> {
            table.insert(finColNames, values);
        };

    }

    private static List<Value> parseValues(Iterator<String> parts, Iterable<String> colNames, Relation relation) throws SqlParseException {
        Map<String, DataType> colDefs = relation.getColumnDefinitions();
        List<Value> vals = new ArrayList<>();
        Iterator<String> colNameIt = colNames.iterator();
        boolean expecting = true;
        while (parts.hasNext()) {
            String val = parts.next();
            if (expecting) {
                if (!colNameIt.hasNext()) {
                    throw new SqlParseException("More values than column names supplied");
                }
                String colName = colNameIt.next();
                DataType dataType = colDefs.get(colName);
                if (dataType == null) {
                    throw new SqlParseException("No column '" + colName + "' exists on table '" + relation.getName() + "'");
                }

                vals.add(parseValue(val, dataType));
            } else {
                if (")".equals(val)) {
                    break;
                } else {
                    assertToken(",", val);
                }
            }
            expecting = !expecting;
        }

        if (colNameIt.hasNext()) {
            throw new SqlParseException("More column names than values supplied");
        }

        return vals;
    }

    private static Value parseValue(String val, DataType dataType) throws SqlParseException {
        try {
            switch (dataType) {
                case STRING:
                    if (!val.startsWith("'") || !val.endsWith("'")) {
                        throw new SqlParseException("Illegal string format: <" + val + "> must be wrapped in single quotes");
                    }
                    return Value.of(val.substring(1, val.length() - 1));
                case INTEGER:
                    return Value.of(Long.parseLong(val));
                case FLOAT:
                    return Value.of(Double.parseDouble(val));
                default:
                    throw new SqlParseException("Unsupported DataType: " + dataType);
            }
        } catch (NumberFormatException e) {
            throw new SqlParseException("Illegal number format for DataType " + dataType + ": '" + val + "'", e);
        }

    }

    private Query parseSelect(Iterator<String> parts) throws SqlParseException {
        try {
            List<String> columnNames = new ArrayList<>();
            boolean all = false;
            boolean expecting = true;
            while (parts.hasNext()) {
                String next = parts.next();
                if (expecting) {
                    expecting = false;
                    if ("*".equals(next)) {
                        all = true;
                    } else {
                        columnNames.add(next);
                    }
                } else {
                    if ("from".equals(next)) {
                        break;
                    } else {
                        assertNextToken(",", parts);
                    }
                }
            }

            if (all) {
                columnNames.clear();
            } else if (columnNames.isEmpty()) {
                throw new SqlParseException("Must specify at least 1 column in SELECT statement");
            }

            String tableName = parts.next();

            Predicate<Tuple> predicate = parseWhere(parts);

            return database -> {
                Table table = database.get(tableName);
                table.select(columnNames, predicate).forEach(tuple -> {
                    System.out.print("|");
                    tuple.forEach(value -> {
                        System.out.print(" " + value + " |");
                    });
                    System.out.println(" |");

                });
            };
        } catch (NoSuchElementException e) {
            throw new SqlParseException("Incomplete sql SELECT statement");
        }

    }

    private static String preProcess(String sql) {
        sql = sql.toLowerCase();
        return sql.replaceAll("=", " = ")
            .replaceAll("\\(", " ( ")
            .replaceAll("\\)", " ) ")
            .replaceAll(",", " , ");
    }


    private Predicate<Tuple> parseWhere(Iterator<String> parts) throws SqlParseException {
        if (parts.hasNext()) {
            assertNextToken("where", parts);

            while (parts.hasNext()) {
                // TODO: implement
                break;
            }
        }
        return null;
    }

    private static void assertNextToken(String expected, Iterator<String> parts) throws SqlParseException {
        assertToken(expected, parts.next());
    }

    private static void assertToken(String expected, String actual) throws SqlParseException {
        if (!expected.equals(actual)) {
            throw new SqlParseException("Expected '" + expected + "' but found '" + actual + "'");
        }
    }


}
