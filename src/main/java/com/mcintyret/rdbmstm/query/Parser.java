package com.mcintyret.rdbmstm.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import com.mcintyret.rdbmstm.Formatter;
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
                    return parseUpdate(parts);
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

    private Query parseUpdate(Iterator<String> parts) throws SqlParseException {
        String tableName = parts.next();
        Table table = database.get(tableName);

        assertNextToken("set", parts);
        List<String> colNames = new ArrayList<>();
        List<Value> values = new ArrayList<>();
        String currentCol = null;

        int i = 0;
        while (parts.hasNext()) {
            String part = parts.next();
            if ("where".equals(part)) {
                break;
            }
            switch (i++ % 3) {
                case 0:
                    colNames.add(currentCol = part);
                    break;
                case 1:
                    assertToken("=", part);
                    break;
                case 2:
                    values.add(parseValue(part, currentCol, table));
                    break;
                default:
                    throw new AssertionError();

            }
        }

        if (colNames.isEmpty()) {
            throw new SqlParseException("Must update at least one column in UPDATE statment");
        } else if (colNames.size() != values.size()) {
            throw new SqlParseException("Different number of column names and new values");
        }

        Predicate<Tuple> predicate = parseWhere(parts);

        return database -> {
            table.update(colNames, values, predicate);
        };
    }

    private Query parseInsert(Iterator<String> parts) throws SqlParseException {
        assertNextToken("into", parts);

        String tableName = parts.next();
        Table table = database.get(tableName);

        Collection<String> cols = parseList(parts, "values", ",", table.getColumnNames(), val -> val);

        assertNextToken("(", parts);
        Collection<Value> values = parseValues(parts, cols, table);

        return database -> {
            table.insert(cols, values);
        };

    }

    private static <T> Collection<T> parseList(Iterator<String> parts, String terminator, String separator, Collection<T> defaultList, ParseFunction<T> func) throws SqlParseException {
        boolean expecting = true;
        List<T> result = new ArrayList<>();
        boolean first = true;
        while (parts.hasNext()) {
            String part = parts.next();
            if (first && terminator.equals(part)) {
                break;
            } else if (expecting) {
                result.add(func.apply(part));
            } else {
                if (terminator.equals(part)) {
                    break;
                } else {
                    assertToken(separator, part);
                }
            }
            first = false;
            expecting = !expecting;
        }

        return result.isEmpty() && defaultList != null ? defaultList : result;
    }

    private static Collection<Value> parseValues(Iterator<String> parts, Iterable<String> colNames, Relation relation) throws SqlParseException {
        Map<String, DataType> colDefs = relation.getColumnDefinitions();
        Iterator<String> colNameIt = colNames.iterator();

        Collection<Value> values = parseList(parts, ")", ",", null, val -> {
            if (!colNameIt.hasNext()) {
                throw new SqlParseException("More values than column names supplied");
            }
            String colName = colNameIt.next();

            return parseValue(val, colName, relation);
        });

        if (colNameIt.hasNext()) {
            throw new SqlParseException("More column names than values supplied");
        }

        return values;
    }

    private static Value parseValue(String val, String colName, Relation relation) throws SqlParseException {
        DataType dataType = relation.getColumnDefinitions().get(colName);
        if (dataType == null) {
            throw new SqlParseException("No column '" + colName + "' exists on table '" + relation.getName() + "'");
        }

        return parseValue(val, dataType);
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
            Collection<String> columnNames = toSelectList(parseList(parts, "from", ",", null, val -> val));

            String tableName = parts.next();

            Predicate<Tuple> predicate = parseWhere(parts);

            return database -> {
                Relation select = database.get(tableName).select(columnNames, predicate);
                System.out.println(Formatter.toString(select));
            };
        } catch (NoSuchElementException e) {
            throw new SqlParseException("Incomplete sql SELECT statement");
        }

    }

    private static Collection<String> toSelectList(Collection<String> in) {
        if (in.size() == 1 && "*".equals(in.iterator().next())) {
            in.clear();
        }
        return in;
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


    @FunctionalInterface
    static interface ParseFunction<T> {

        T apply(String str) throws SqlParseException;

    }
}
