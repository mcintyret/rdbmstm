package com.mcintyret.rdbmstm.query;

import static com.mcintyret.rdbmstm.collect.CollectUtils.toMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

import com.mcintyret.rdbmstm.collect.PeekableIterator;
import com.mcintyret.rdbmstm.core.ColumnDefinition;
import com.mcintyret.rdbmstm.core.DataType;
import com.mcintyret.rdbmstm.core.Database;
import com.mcintyret.rdbmstm.core.Relation;
import com.mcintyret.rdbmstm.core.Table;
import com.mcintyret.rdbmstm.core.Tuple;
import com.mcintyret.rdbmstm.core.Value;
import com.mcintyret.rdbmstm.core.predicate.ColumnEquals;
import com.mcintyret.rdbmstm.core.predicate.ColumnGreaterThan;
import com.mcintyret.rdbmstm.core.predicate.ColumnLessThan;
import com.mcintyret.rdbmstm.core.select.AliasingSelector;
import com.mcintyret.rdbmstm.core.select.FilteringSelector;
import com.mcintyret.rdbmstm.core.select.OrderingSelector;
import com.mcintyret.rdbmstm.core.select.SelectingSelector;
import com.mcintyret.rdbmstm.core.select.Selector;
import com.mcintyret.rdbmstm.core.select.aggregate.AggregatingFunction;
import com.mcintyret.rdbmstm.core.select.aggregate.Aggregators;

// TODO: split this class out!
public class Parser {

    private final Database database;

    public Parser(Database database) {
        this.database = database;
    }

    public Execution parse(String sql) throws SqlParseException {
        PeekableIterator<String> parts = preProcess(sql);
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
                    return parseDelete(parts);
                case "insert":
                    return parseInsert(parts);
                default:
                    throw new SqlParseException("Cannot parse query: " + sql);
            }
        } catch (NumberFormatException e) {
            throw new SqlParseException("Error parsing invalid number format", e);
        } catch (NoSuchElementException e) {
            throw new SqlParseException("SQL statement truncated unexpectedly: " + sql);
        } catch (SqlParseException e) {
            throw new SqlParseException("Error parsing sql '" + sql + "': " + e.getMessage(), e);
        }

        throw new SqlParseException("Not handled yet: " + sql);
    }

    private Execution parseDelete(PeekableIterator<String> parts) {
        assertNextToken("from", parts);

        Table table = database.get(parts.next());

        Predicate<Tuple> predicate = parseWhere(parts);

        return Execution.forModification(() -> Modification.delete(table.delete(predicate)));
    }

    private Execution parseUpdate(Iterator<String> parts) throws SqlParseException {
        String tableName = parts.next();
        Table table = database.get(tableName);

        assertNextToken("set", parts);
        Map<String, Value> values = new HashMap<>();
        String currentCol = null;

        int i = 0;
        while (parts.hasNext()) {
            String part = parts.next();
            if ("where".equals(part)) {
                break;
            }
            switch (i++ % 4) {
                case 0:
                    currentCol = part;
                    break;
                case 1:
                    assertToken("=", part);
                    break;
                case 2:
                    values.put(currentCol, parseValue(part, currentCol, table));
                    break;
                case 3:
                    assertToken(",", part);
                    break;
                default:
                    throw new AssertionError();

            }
        }

        if (values.isEmpty()) {
            throw new SqlParseException("Must update at least one column in UPDATE statment");
        }

        Predicate<Tuple> predicate = parsePredicate(parts);

        return Execution.forModification(() -> { return Modification.update(table.update(values, predicate)); });
    }

    private Execution parseInsert(Iterator<String> parts) throws SqlParseException {
        assertNextToken("into", parts);

        String tableName = parts.next();
        Table table = database.get(tableName);

        Collection<String> cols = parseList(parts, "values", ",", table.getColumnNames(), val -> val);

        assertNextToken("(", parts);
        Collection<Value> values = parseValues(parts, cols, table);

        return Execution.forModification(() -> {
            table.insert(toMap(cols, values));
            return Modification.insert();
        });

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
        ColumnDefinition cd = relation.getColumnDefinitions().get(colName);
        if (cd == null) {
            throw new SqlParseException("No column '" + colName + "' exists on table");
        }

        return parseValue(val, cd.getDataType());
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

    private Execution parseSelect(PeekableIterator<String> parts) throws SqlParseException {
        try {
            Map<String, String> aliases = new HashMap<>();
            Map<String, AggregatingFunction> functions = new HashMap<>();
            Set<String> cols = new LinkedHashSet<>();
            boolean all = false;
            String name = null;
            String alias = null;
            AggregatingFunction func;
            boolean as = false;
            while (parts.hasNext()) {
                String part = parts.next();
                if ("*".equals(part)) {
                    all = true;
                } else if ("from".equals(part) || ",".equals(part)) {
                    if (!all && name == null) {
                        throw new SqlParseException("Column name not specified");
                    }
                    if (as && alias == null) {
                        throw new SqlParseException("No alias specified after AS for column '" + name + "'");
                    }
                    if (name != null) {
                        if (alias != null && aliases.put(alias, name) != null) {
                            throw new SqlParseException("Duplicate alias provided: " + alias);
                        }
                        if (!cols.add(alias == null ? name : alias)) {
                            throw new SqlParseException("Duplicate column name selected: " + name);
                        }
                    }
                    if ("from".equals(part)) {
                        break;
                    } else {
                        name = alias = null;
                        func = null;
                        as = false;
                    }
                } else if ("as".equals(part)) {
                    as = true;
                } else {
                    if (name == null) {
                        func = Aggregators.getAggregator(name);
                        if (func == null) {
                            name = part;
                        } else {
                            assertNextToken("(", parts);
                            name = parts.next();
                            assertNextToken(")", parts);
                            functions.put(name, func);
                        }
                    } else {
                        alias = part;
                    }
                }
            }

            if (all && (!cols.isEmpty() || !aliases.isEmpty())) {
                throw new SqlParseException("Cannot select * as well as named columns");
            }

            Table table = database.get(parts.next());

            Predicate<Tuple> predicate = parseWhere(parts);

            Comparator<Tuple> comp = parseOrderBy(parts);

            return Execution.forQuery(() -> makeSelector(cols, aliases, predicate, comp).select(table));
        } catch (NoSuchElementException e) {
            throw new SqlParseException("Incomplete sql SELECT statement");
        }

    }

    private Selector makeSelector(Set<String> columns, Map<String, String> aliases, Predicate<Tuple> predicate, Comparator<Tuple> comp) {
        Selector selector = aliases.isEmpty() ? (Selector) relation -> {return relation;} : new AliasingSelector(aliases);

        if (predicate != null) {
            selector = selector.chain(new FilteringSelector(predicate));
        }
        if (comp != null) {
            selector = selector.chain(new OrderingSelector(comp));
        }
        if (!columns.isEmpty()) {
            selector = selector.chain(new SelectingSelector(columns));
        }

        return selector;
    }

    private Comparator<Tuple> parseOrderBy(PeekableIterator <String> parts) {
        if (consumeIfPresent("order", parts)) {
            assertNextToken("by", parts);
            Comparator<Tuple> comp = columnComparator(parts.next());

            if (parts.hasNext()) {
                String dir = parts.next();
                switch (dir) {
                    case "asc":
                    case "ascending":
                        break;
                    case "desc":
                    case "descending":
                        comp = comp.reversed();
                        break;
                    default:
                        throw new SqlParseException("Illegal modifier for ORDER BY: " + dir);

                }
            }
            return comp;
        }
        return null;
    }

    private static final String SPACE_CHARS = "=,()<>";

    private static PeekableIterator<String> preProcess(final String sql) {
        return new PeekableIterator<String>() {

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
                    if (i == sql.length()) {
                        if (c == ';') {
                            return sb == null ? null : sb.toString();
                        } else {
                            throw new SqlParseException("SQL did not end with ';'");
                        }
                    }

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

            @Override
            public String peek() {
                if (hasNext()) {
                    return next;
                }
                throw new NoSuchElementException();
            }
        };
    }


    private Predicate<Tuple> parseWhere(PeekableIterator<String> parts) throws SqlParseException {
        if (consumeIfPresent("where", parts)) {

            return parsePredicate(parts);
        }
        return null;
    }

    // TODO: support AND and OR without brackets
    private Predicate<Tuple> parsePredicate(Iterator<String> parts) throws SqlParseException {
        String part = parts.next();

        if ("not".equals(part)) {
            return parsePredicate(parts).negate();
        }

        if ("(".equals(part)) {
            Predicate<Tuple> left = parsePredicate(parts);

            boolean and = parseAndOr(parts);

            Predicate<Tuple> right = parsePredicate(parts);

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
                break;
            }
        }

        PredicateType type = PredicateType.PARTS_MAP.get(pps);
        if (type == null) {
            throw new SqlParseException("Unknown predicate in WHERE clause");
        }
        return type.makePredicate(colName, val);
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
            Predicate<Tuple> makePredicate(String colName, String val) {
                return new ColumnEquals(colName, val);
            }
        },
        NE(PredicatePart.LT, PredicatePart.GT) {
            @Override
            Predicate<Tuple> makePredicate(String colName, String val) {
                return EQ.makePredicate(colName, val).negate();
            }
        },
        GT(PredicatePart.GT) {
            @Override
            Predicate<Tuple> makePredicate(String colName, String val) {
                return new ColumnGreaterThan(colName, Double.parseDouble(val));
            }
        },
        GTE(PredicatePart.GT, PredicatePart.EQ) {
            @Override
            Predicate<Tuple> makePredicate(String colName, String val) {
                return LT.makePredicate(colName, val).negate();
            }
        },
        LT(PredicatePart.LT) {
            @Override
            Predicate<Tuple> makePredicate(String colName, String val) {
                return new ColumnLessThan(colName, Double.parseDouble(val));
            }
        },
        LTE(PredicatePart.LT, PredicatePart.EQ) {
            @Override
            Predicate<Tuple> makePredicate(String colName, String val) {
                return GT.makePredicate(colName, val).negate();
            }
        };

        private final List<PredicatePart> parts;

        PredicateType(PredicatePart... parts) {
            this.parts = Arrays.asList(parts);
        }

        abstract Predicate<Tuple> makePredicate(String colName, String val);

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

    private static boolean consumeIfPresent(String test, PeekableIterator<String> parts) {
        if (parts.hasNext() && test.equals(parts.peek())) {
            parts.next();
            return true;
        }
        return false;
    }


    @FunctionalInterface
    static interface ParseFunction<T> {

        T apply(String str) throws SqlParseException;

    }

    private static Comparator<Tuple> columnComparator(final String columnName) {
        return (left, right) -> left.select(columnName).compareTo(right.select(columnName));
    }

}
