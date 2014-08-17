package com.mcintyret.rdbmstm.core;

import static com.mcintyret.rdbmstm.core.TableTestUtils.assertIterablesEqual;
import static com.mcintyret.rdbmstm.core.TableTestUtils.assertOrderedRelationsEqual;
import static com.mcintyret.rdbmstm.core.TableTestUtils.assertRelationsEqual;
import static com.mcintyret.rdbmstm.core.TableTestUtils.toRelation;
import static java.util.Arrays.asList;

import java.util.LinkedHashMap;
import java.util.Map;

import org.testng.annotations.Test;

@Test
public class SelectTest extends BaseTableTest {

    public void shouldSelectInColumnDefinitionOrderForStar() {
        Relation all = parser.parse("select * from table_1;").executeQuery();

        assertIterablesEqual(all.getColumnNames(), asList("foo", "bar", "baz"));
    }

    public void shouldSelectAllForStar() {
        Relation all = parser.parse("select * from table_1;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {15.46, 17, "testing"},
            {13.0, 27, "foo"},
            {57735.12, 1, "this"},
            {0.0007, 3486978, "is"},
            {1556.345, 8975, "testing"}
        };

        assertRelationsEqual(all, toRelation(colDefs, expectedTable));
    }

    public void shouldSelectWhereOneColumnEquals() {
        Relation all = parser.parse("select * from table_1 where baz = 'testing';").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {15.46, 17, "testing"},
            {1556.345, 8975, "testing"}
        };

        assertRelationsEqual(all, toRelation(colDefs, expectedTable));

    }

    public void shouldSelectWhereTwoColumnEqualsAnd() {
        Relation all = parser.parse("select * from table_1 where (baz = 'testing' and bar = 17);").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {15.46, 17, "testing"},
        };

        assertRelationsEqual(all, toRelation(colDefs, expectedTable));
    }

    public void shouldSelectWhereTwoColumnEqualsOr() {
        Relation all = parser.parse("select * from table_1 where (baz = 'testing' or bar = 27);").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {15.46, 17, "testing"},
            {13.0, 27, "foo"},
            {1556.345, 8975, "testing"}
        };

        assertRelationsEqual(all, toRelation(colDefs, expectedTable));
    }

    public void shouldSelectCertainColumns() {
        Relation all = parser.parse("select baz, foo from table_1;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {"testing", 15.46},
            {"foo", 13.0},
            {"this", 57735.12},
            {"is", 0.0007},
            {"testing", 1556.345}
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("baz", colDefs.get("baz"));
        newColDefs.put("foo", colDefs.get("foo"));

        assertRelationsEqual(all, toRelation(newColDefs, expectedTable));
    }

    public void shouldSelectCertainColumnsWithWhereClause() {
        Relation all = parser.parse("select baz, foo from table_1 where foo > 20;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {"this", 57735.12},
            {"testing", 1556.345}
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("baz", colDefs.get("baz"));
        newColDefs.put("foo", colDefs.get("foo"));

        assertRelationsEqual(all, toRelation(newColDefs, expectedTable));
    }

    public void shouldSelectColumnsAllAliased() {
        Relation all = parser.parse("select baz one, foo as two, bar three from table_1;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {"testing", 15.46, 17},
            {"foo", 13.0, 27},
            {"this", 57735.12, 1},
            {"is", 0.0007, 3486978},
            {"testing", 1556.345, 8975}
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("one", colDefs.get("baz"));
        newColDefs.put("two", colDefs.get("foo"));
        newColDefs.put("three", colDefs.get("bar"));

        assertRelationsEqual(all, toRelation(newColDefs, expectedTable));
    }

    public void shouldSelectColumnsNotAllAliased() {
        Relation all = parser.parse("select baz one, foo, bar three from table_1;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {"testing", 15.46, 17},
            {"foo", 13.0, 27},
            {"this", 57735.12, 1},
            {"is", 0.0007, 3486978},
            {"testing", 1556.345, 8975}
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("one", colDefs.get("baz"));
        newColDefs.put("foo", colDefs.get("foo"));
        newColDefs.put("three", colDefs.get("bar"));

        assertRelationsEqual(all, toRelation(newColDefs, expectedTable));
    }

    public void shouldSelectColumnsNotAllAliasedColumnsRepeated() {
        Relation all = parser.parse("select baz one, foo, bar three, baz, foo two from table_1;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {"testing", 15.46, 17, "testing", 15.46},
            {"foo", 13.0, 27, "foo", 13.0},
            {"this", 57735.12, 1, "this", 57735.12},
            {"is", 0.0007, 3486978, "is", 0.0007},
            {"testing", 1556.345, 8975, "testing", 1556.345}
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("one", colDefs.get("baz"));
        newColDefs.put("foo", colDefs.get("foo"));
        newColDefs.put("three", colDefs.get("bar"));
        newColDefs.put("baz", colDefs.get("baz"));
        newColDefs.put("two", colDefs.get("foo"));

        assertRelationsEqual(all, toRelation(newColDefs, expectedTable));
    }

    public void shouldSelectColumnsNotAllAliasedColumnsRepeatedWithAlias() {
        Relation all = parser.parse("select baz one, foo, bar three, baz, foo two, bar four from table_1;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {"testing", 15.46, 17, "testing", 15.46, 17},
            {"foo", 13.0, 27, "foo", 13.0, 27},
            {"this", 57735.12, 1, "this", 57735.12, 1},
            {"is", 0.0007, 3486978, "is", 0.0007, 3486978},
            {"testing", 1556.345, 8975, "testing", 1556.345, 8975}
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("one", colDefs.get("baz"));
        newColDefs.put("foo", colDefs.get("foo"));
        newColDefs.put("three", colDefs.get("bar"));
        newColDefs.put("baz", colDefs.get("baz"));
        newColDefs.put("two", colDefs.get("foo"));
        newColDefs.put("four", colDefs.get("bar"));

        assertRelationsEqual(all, toRelation(newColDefs, expectedTable));
    }

    public void shouldSelectColumnsNotAllAliasedWhereOnAliasedColumn() {
        Relation all = parser.parse("select baz one, foo, bar three from table_1 where three > 20;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {"foo", 13.0, 27},
            {"is", 0.0007, 3486978},
            {"testing", 1556.345, 8975}
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("one", colDefs.get("baz"));
        newColDefs.put("foo", colDefs.get("foo"));
        newColDefs.put("three", colDefs.get("bar"));

        assertRelationsEqual(all, toRelation(newColDefs, expectedTable));
    }

    public void shouldSelectColumnsNotAllAliasedWhereOnAliasedAndNonAliasedColumn() {
        Relation all = parser.parse("select baz one, foo, bar three from table_1 where (three > 20 and foo < 20);").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {"foo", 13.0, 27},
            {"is", 0.0007, 3486978},
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("one", colDefs.get("baz"));
        newColDefs.put("foo", colDefs.get("foo"));
        newColDefs.put("three", colDefs.get("bar"));

        assertRelationsEqual(all, toRelation(newColDefs, expectedTable));
    }

    public void shouldSelectCertainColumnsWithoutDuplicateTuples() {
        Relation all = parser.parse("select baz from table_1;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {"foo"},
            {"this"},
            {"is"},
            {"testing"}
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("baz", colDefs.get("baz"));

        assertRelationsEqual(all, toRelation(newColDefs, expectedTable));
    }

    public void shouldOrderByAscending() {
        Relation all = parser.parse("select * from table_1 order by foo asc;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {0.0007, 3486978, "is"},
            {13.0, 27, "foo"},
            {15.46, 17, "testing"},
            {1556.345, 8975, "testing"},
            {57735.12, 1, "this"}
        };

        assertOrderedRelationsEqual(all, toRelation(colDefs, expectedTable));
    }

    public void shouldOrderByAscendingDefault() {
        Relation all = parser.parse("select * from table_1 order by foo;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {0.0007, 3486978, "is"},
            {13.0, 27, "foo"},
            {15.46, 17, "testing"},
            {1556.345, 8975, "testing"},
            {57735.12, 1, "this"}
        };

        assertOrderedRelationsEqual(all, toRelation(colDefs, expectedTable));
    }

    public void shouldOrderByDescending() {
        Relation all = parser.parse("select * from table_1 order by foo desc;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {57735.12, 1, "this"},
            {1556.345, 8975, "testing"},
            {15.46, 17, "testing"},
            {13.0, 27, "foo"},
            {0.0007, 3486978, "is"}
        };

        assertOrderedRelationsEqual(all, toRelation(colDefs, expectedTable));
    }

    public void shouldOrderByAscendingString() {
        Relation all = parser.parse("select * from table_1 where bar <> 17 order by baz asc;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {13.0, 27, "foo"},
            {0.0007, 3486978, "is"},
            {1556.345, 8975, "testing"},
            {57735.12, 1, "this"}
        };

        assertOrderedRelationsEqual(all, toRelation(colDefs, expectedTable));
    }

    public void shouldOrderByAliasedColumn() {
        Relation all = parser.parse("select foo, bar, baz as bam from table_1 where bar <> 17 order by bam asc;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {13.0, 27, "foo"},
            {0.0007, 3486978, "is"},
            {1556.345, 8975, "testing"},
            {57735.12, 1, "this"}
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("foo", colDefs.get("foo"));
        newColDefs.put("bar", colDefs.get("bar"));
        newColDefs.put("bam", colDefs.get("baz"));

        assertOrderedRelationsEqual(all, toRelation(newColDefs, expectedTable));
    }

    public void shouldOrderByAliasedColumnFiltered() {
        Relation all = parser.parse("select foo shoe, bar, baz as bam from table_1 where (shoe > 14 and bar <> 17) order by bam asc;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {1556.345, 8975, "testing"},
            {57735.12, 1, "this"}
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("shoe", colDefs.get("foo"));
        newColDefs.put("bar", colDefs.get("bar"));
        newColDefs.put("bam", colDefs.get("baz"));

        assertOrderedRelationsEqual(all, toRelation(newColDefs, expectedTable));
    }

    public void shouldFilterOnColumnNotSelected() {
        Relation all = parser.parse("select bar from table_1 where foo > 14;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {17},
            {1},
            {8975}
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("bar", colDefs.get("bar"));

        assertRelationsEqual(all, toRelation(newColDefs, expectedTable));

    }

    public void shouldOrderByOnColumnNotSelected() {
        Relation all = parser.parse("select bar from table_1 order by baz;").executeQuery();

        Object[][] expectedTable = new Object[][]{
            {27},
            {3486978},
            {8975},
            {17},
            {1}
        };

        Map<String, ColumnDefinition> newColDefs = new LinkedHashMap<>();
        newColDefs.put("bar", colDefs.get("bar"));

        assertRelationsEqual(all, toRelation(newColDefs, expectedTable));

    }

}
