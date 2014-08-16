package com.mcintyret.rdbmstm.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import com.mcintyret.rdbmstm.core.predicate.ColumnEquals;
import com.mcintyret.rdbmstm.core.predicate.ColumnGreaterThan;
import com.mcintyret.rdbmstm.core.predicate.ColumnLessThan;

public class Parser {

    private final Database database;

    public Parser(Database database) {
        this.database = database;
    }

    public Query parse(String sql) throws SqlParseException {
        Iterator<String> parts = preProcess2(sql);
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

        Predicate<Tuple> predicate = parseWhere(parts, table);

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
        if ("null".equals(val)) {
            return Value.nullOf(dataType);
        }
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

            Table table = database.get(parts.next());

            Predicate<Tuple> predicate = parseWhere(parts, table);

            return database -> {
                Relation select = table.select(columnNames, predicate);
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

    private static final String SPACE_CHARS = "=,()<>";

    private static Iterator<String> preProcess2(final String sql) {
        return new Iterator<String>() {

            int i = 0;
            String next = null;

            @Override
            public boolean hasNext() {
                if (next == null) {
                    next = findNext();
                }
                return next != null;
            }

            private String findNext() {
                StringBuilder sb = null;
                boolean inQuotes = false;
                while (i < sql.length()) {
                    char c = sql.charAt(i++);
                    if (!inQuotes) {

                        if (Character.isWhitespace(c)) {
                            if (sb != null) {
                                return sb.toString();
                            }
                            continue;
                        }
                        if (SPACE_CHARS.indexOf(c) >= 0) {
                            if (sb != null) {
                                i--; // we want to see this again!
                                return sb.toString();
                            }
                            return String.valueOf(c);
                        }

                        if (sb == null) {
                            sb = new StringBuilder();
                        }
                    }
                    sb.append(c);

                    if (c == '\'') {
                        inQuotes = !inQuotes;
                    }
                }
                if (sb != null) {
                    return sb.toString();
                }

                return null;
            }

            @Override
            public String next() {
                if (hasNext()) {
                    String ret = next;
                    next = null;
                    return ret;
                }
                throw new NoSuchElementException();
            }
        };
    }


    private Predicate<Tuple> parseWhere(Iterator<String> parts, Relation relation) throws SqlParseException {
        if (parts.hasNext()) {
            assertNextToken("where", parts);

            try {
                return parsePredicate(parts, relation);
            } catch (NoSuchElementException e) {
                throw new SqlParseException("WHERE clause incomplete or not formatted propertly");
            }

        }
        return null;
    }

    private Predicate<Tuple> parsePredicate(Iterator<String> parts, Relation relation) throws SqlParseException {
        String part = parts.next();

        if ("not".equals(part)) {
            return parsePredicate(parts, relation).negate();
        }

        if ("(".equals(part)) {
            Predicate<Tuple> left = parsePredicate(parts, relation);

            boolean and = parseAndOr(parts);

            Predicate<Tuple> right = parsePredicate(parts, relation);

            assertNextToken(")", parts); // consume closing parenthesis

            return and ? left.and(right) : left.or(right);
        }

        // Simple column predicates for now
        String colName = part;

        List<PredicatePart> pps = new ArrayList<>();
        String val = null;
        while (parts.hasNext()) {
            part = parts.next();
            PredicatePart pp = PredicatePart.PARTS.get(part);
            if (pp != null) {
                pps.add(pp);
            } else {
                val = part;
            }
        }

        PredicateType type = PredicateType.PARTS_MAP.get(pps);
        if (type == null) {
            throw new SqlParseException("Unknown predicate in WHERE clause");
        }
        return type.makePredicate(colName, parseValue(val, colName, relation));
    }

    // TODO handle IS and IS NOT
    enum PredicatePart {
        EQ("="),
        LT("<"),
        GT(">");

        final String val;

        PredicatePart(String val) {
            this.val = val;
        }

        static final Map<String, PredicatePart> PARTS = makeParts();

        private static Map<String, PredicatePart> makeParts() {
            Map<String, PredicatePart> parts = new HashMap<>();
            for (PredicatePart predicateParts : values()) {
                parts.put(predicateParts.val, predicateParts);
            }
            return Collections.unmodifiableMap(parts);
        }
    }

    enum PredicateType {
        EQ(PredicatePart.EQ) {
            @Override
            Predicate<Tuple> makePredicate(String colName, Value val) {
                return new ColumnEquals(colName, val);
            }
        },
        NE(PredicatePart.LT, PredicatePart.GT) {
            @Override
            Predicate<Tuple> makePredicate(String colName, Value val) {
                return EQ.makePredicate(colName, val).negate();
            }
        },
        GT(PredicatePart.GT) {
            @Override
            Predicate<Tuple> makePredicate(String colName, Value val) {
                return new ColumnGreaterThan(colName, val);
            }
        },
        GTE(PredicatePart.GT, PredicatePart.EQ) {
            @Override
            Predicate<Tuple> makePredicate(String colName, Value val) {
                return LT.makePredicate(colName, val).negate();
            }
        },
        LT(PredicatePart.LT) {
            @Override
            Predicate<Tuple> makePredicate(String colName, Value val) {
                return new ColumnLessThan(colName, val);
            }
        },
        LTE(PredicatePart.LT, PredicatePart.EQ) {
            @Override
            Predicate<Tuple> makePredicate(String colName, Value val) {
                return GT.makePredicate(colName, val).negate();
            }
        };

        private final List<PredicatePart> parts;

        PredicateType(PredicatePart... parts) {
            this.parts = Arrays.asList(parts);
        }

        abstract Predicate<Tuple> makePredicate(String colName, Value val);

        static final Map<List<PredicatePart>, PredicateType> PARTS_MAP = makePartsMap();

        private static Map<List<PredicatePart>, PredicateType> makePartsMap() {
            Map<List<PredicatePart>, PredicateType> map = new HashMap<>();
            for (PredicateType pt : values()) {
                map.put(pt.parts, pt);
            }
            return Collections.unmodifiableMap(map);
        }

    }

    private boolean parseAndOr(Iterator<String> parts) throws SqlParseException {
        String part = parts.next();
        switch (part) {
            case "and":
                return true;
            case "or":
                return false;
            default:
                throw new SqlParseException("Expected AND or OR but got '" + part);
        }
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
