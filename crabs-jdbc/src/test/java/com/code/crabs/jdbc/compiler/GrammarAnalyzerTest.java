package com.code.crabs.jdbc.compiler;

import com.code.crabs.jdbc.lang.Statement;
import com.code.crabs.jdbc.lang.expression.Constant;
import com.code.crabs.jdbc.lang.expression.Reference;
import com.code.crabs.jdbc.lang.extension.statement.SelectStatement;
import com.code.crabs.exception.SQL4ESException;
import com.code.crabs.jdbc.lang.extension.clause.*;
import com.code.crabs.jdbc.lang.extension.expression.*;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.*;

public class GrammarAnalyzerTest {

    @Test
    public void testAnalyzeSelectStatement_OK_ColumnReference() throws Exception {
        final String sql = "SELECT ID, NAME FROM STUDENT";
        final Statement actual = GrammarAnalyzer.analyze(sql);
        final Statement expected = new SelectStatement(
                new SelectClause(
                        false,
                        null,
                        new SelectClause.ResultColumnDeclare((String) null, new Reference(null, "ID")),
                        new SelectClause.ResultColumnDeclare((String) null, new Reference(null, "NAME"))
                ),
                new FromClause(
                        new FromClause.SimpleTableDeclare(null, "STUDENT")
                ),
                null,
                null,
                null,
                null,
                null
        );
        assertEquals(expected, actual);
    }

    @Test
    public void testAnalyzeSelectStatement_OK_ColumnReferenceAndTableAlias() throws Exception {
        final String sql = "SELECT `ID`, `NAME` FROM STUDENT STU";
        final Statement actual = GrammarAnalyzer.analyze(sql);
        final Statement expected = new SelectStatement(
                new SelectClause(
                        false,
                        null,
                        new SelectClause.ResultColumnDeclare((String) null, new Reference(null, "ID")),
                        new SelectClause.ResultColumnDeclare((String) null, new Reference(null, "NAME"))
                ),
                new FromClause(
                        new FromClause.SimpleTableDeclare("STU", "STUDENT")
                ),
                null,
                null,
                null,
                null,
                null
        );
        assertEquals(expected, actual);
    }

    @Test
    public void testAnalyzeSelectStatement_OK_ColumnReferenceAndColumnAliasAndTableAlias() throws Exception {
        final String sql = "SELECT STU.`ID` AS STU_ID, `NAME` FROM STUDENT STU";
        final Statement actual = GrammarAnalyzer.analyze(sql);
        final Statement expected = new SelectStatement(
                new SelectClause(
                        false,
                        null,
                        new SelectClause.ResultColumnDeclare("STU_ID", new Reference("STU", "ID")),
                        new SelectClause.ResultColumnDeclare((String) null, new Reference(null, "NAME"))
                ),
                new FromClause(
                        new FromClause.SimpleTableDeclare("STU", "STUDENT")
                ),
                null,
                null,
                null,
                null,
                null
        );
        assertEquals(expected, actual);
    }

    @Test
    public final void testAnalyzeSelectStatement_OK_OK_AllColumnReference() throws SQLException {
        final String sql = "select * from STUDENT stu";
        final Statement actual = GrammarAnalyzer.analyze(sql);
        final Statement expected = new SelectStatement(
                new SelectClause(
                        null,
                        null,
                        new SelectClause.ResultColumnDeclare(
                                (String) null,
                                new Reference((String) null, Reference.ALL_COLUMN_IDENTIFIER)
                        )
                ),
                new FromClause(new FromClause.SimpleTableDeclare("stu", "STUDENT")),
                null,
                null,
                null,
                null,
                null
        );
        assertEquals(expected, actual);
    }

    @Test
    public final void testAnalyzeSelectStatement_OK_WhereCondition_InExpression() throws SQLException {
        final String sql = "SELECT * FROM STUDENT WHERE ID IN ('10010', '10011', '10012')";
        final Statement actual = GrammarAnalyzer.analyze(sql);
        final Statement expected = new SelectStatement(
                new SelectClause(
                        null,
                        null,
                        new SelectClause.ResultColumnDeclare((String) null, new Reference((String) null, Reference.ALL_COLUMN_IDENTIFIER))
                ),
                new FromClause(new FromClause.SimpleTableDeclare(null, "STUDENT")),
                new WhereClause(
                        new InExpression(
                                new Reference(null, "ID"),
                                new Constant("10010"),
                                new Constant("10011"),
                                new Constant("10012")
                        )
                ),
                null,
                null,
                null,
                null
        );
        assertEquals(expected, actual);
    }

    @Test
    public final void testAnalyzeSelectStatement_OK_WhereCondition_AndExpression() throws SQLException {
        final String sql = "select * from bjhc_16799 where time >= '2014-07-31' and time <= '2014-08-07 23:59:59'";
        final Statement actual = GrammarAnalyzer.analyze(sql);
        final Statement expected = new SelectStatement(
                new SelectClause(
                        null,
                        null,
                        new SelectClause.ResultColumnDeclare((String) null, new Reference((String) null, Reference.ALL_COLUMN_IDENTIFIER))
                ),
                new FromClause(new FromClause.SimpleTableDeclare(null, "bjhc_16799")),
                new WhereClause(
                        new AndExpression(
                                new GreaterThanOrEqualToExpression(
                                        new Reference(null, "time"),
                                        new Constant("2014-07-31")
                                ),
                                new LessThanOrEqualToExpression(
                                        new Reference(null, "time"),
                                        new Constant("2014-08-07 23:59:59")
                                )
                        )
                ),
                null,
                null,
                null,
                null
        );
        assertEquals(expected, actual);
    }

    @Test
    public final void testAnalyzeSelectStatement_OK_WhereCondition_OrExpression() throws SQLException {
        final String sql = "select * from bjhc_16799 where time >= '2014-07-31' and time <= '2014-08-07 23:59:59' " +
                "or logContent like '%thimphu_order_available%'";
        final Statement actual = GrammarAnalyzer.analyze(sql);
        final Statement expected = new SelectStatement(
                new SelectClause(
                        null,
                        null,
                        new SelectClause.ResultColumnDeclare((String) null, new Reference((String) null, Reference.ALL_COLUMN_IDENTIFIER))
                ),
                new FromClause(new FromClause.SimpleTableDeclare(null, "bjhc_16799")),
                new WhereClause(
                        new OrExpression(
                                new AndExpression(
                                        new GreaterThanOrEqualToExpression(
                                                new Reference(null, "time"),
                                                new Constant("2014-07-31")
                                        ),
                                        new LessThanOrEqualToExpression(
                                                new Reference(null, "time"),
                                                new Constant("2014-08-07 23:59:59")
                                        )
                                ),
                                new LikeExpression(
                                        new Reference(null, "logContent"),
                                        new Constant("%thimphu_order_available%")
                                )
                        )
                ),
                null,
                null,
                null,
                null
        );
        assertEquals(expected, actual);
    }

    @Test
    public final void testAnalyzeSelectStatement_OK_Group() throws SQLException, SQL4ESException {
        final String sql = "select id, count(id) from student group by id";
        final Statement actual = GrammarAnalyzer.analyze(sql);
        final Statement expected = new SelectStatement(
                new SelectClause(
                        null,
                        null,
                        new SelectClause.ResultColumnDeclare(
                                (String) null,
                                new Reference(null, "id")
                        ),
                        new SelectClause.ResultColumnDeclare(
                                (String) null,
                                new CountFunction(new Reference(null, "id"))
                        )
                ),
                new FromClause(
                        new FromClause.SimpleTableDeclare(null, "student")
                ),
                null,
                new GroupByClause(
                        new Reference(null, "id")
                ),
                null,
                null,
                null
        );
        assertEquals(expected, actual);
    }

    @Test
    public final void testAnalyzeSelectStatement_OK_Having() throws SQLException, SQL4ESException {
        final String sql = "select id, count(id), max(score) max_score from student group by class having max_score >= 95";
        final Statement actual = GrammarAnalyzer.analyze(sql);
        final Statement expected = new SelectStatement(
                new SelectClause(
                        null,
                        null,
                        new SelectClause.ResultColumnDeclare(
                                (String) null,
                                new Reference(null, "id")
                        ),
                        new SelectClause.ResultColumnDeclare(
                                (String) null,
                                new CountFunction(new Reference(null, "id"))
                        ),
                        new SelectClause.ResultColumnDeclare(
                                "max_score",
                                new MaxinumFunction(new Reference(null, "score"))
                        )
                ),
                new FromClause(new FromClause.SimpleTableDeclare(
                        null,
                        "student"
                )),
                null,
                new GroupByClause(new Reference(null, "class")),
                new HavingClause(new GreaterThanOrEqualToExpression(
                        new Reference(null, "max_score"),
                        new Constant(95)
                )),
                null,
                null
        );
        assertEquals(expected, actual);
    }

    @Test
    public final void testAnalyzeSelectStatement_OK_OrderBy() throws SQLException, SQL4ESException {
        final String sql = "select id, count(id), max(score) max_score from student " +
                "group by class having max_score >= 95 order by max_score desc";
        final Statement actual = GrammarAnalyzer.analyze(sql);
        final Statement expected = new SelectStatement(
                new SelectClause(
                        null,
                        null,
                        new SelectClause.ResultColumnDeclare(
                                (String) null,
                                new Reference(null, "id")
                        ),
                        new SelectClause.ResultColumnDeclare(
                                (String) null,
                                new CountFunction(new Reference(null, "id"))
                        ),
                        new SelectClause.ResultColumnDeclare(
                                "max_score",
                                new MaxinumFunction(new Reference(null, "score"))
                        )
                ),
                new FromClause(new FromClause.SimpleTableDeclare(
                        null,
                        "student"
                )),
                null,
                new GroupByClause(new Reference(null, "class")),
                new HavingClause(new GreaterThanOrEqualToExpression(
                        new Reference(null, "max_score"),
                        new Constant(95)
                )),
                new OrderByClause(new OrderByClause.OrderSpecification(
                        new Reference(null, "max_score"),
                        false
                )),
                null
        );
        assertEquals(expected, actual);
    }

    @Test
    public final void testAnalyzeSelectStatement_OK_Limit() throws SQLException, SQL4ESException {
        final String sql = "select id, count(id), max(score) max_score from student " +
                "group by class having max_score >= 95 order by max_score desc " +
                "limit 0, 10";
        final Statement actual = GrammarAnalyzer.analyze(sql);
        final Statement expected = new SelectStatement(
                new SelectClause(
                        null,
                        null,
                        new SelectClause.ResultColumnDeclare(
                                (String) null,
                                new Reference(null, "id")
                        ),
                        new SelectClause.ResultColumnDeclare(
                                (String) null,
                                new CountFunction(new Reference(null, "id"))
                        ),
                        new SelectClause.ResultColumnDeclare(
                                "max_score",
                                new MaxinumFunction(new Reference(null, "score"))
                        )
                ),
                new FromClause(new FromClause.SimpleTableDeclare(
                        null,
                        "student"
                )),
                null,
                new GroupByClause(new Reference(null, "class")),
                new HavingClause(new GreaterThanOrEqualToExpression(
                        new Reference(null, "max_score"),
                        new Constant(95)
                )),
                new OrderByClause(new OrderByClause.OrderSpecification(
                        new Reference(null, "max_score"),
                        false
                )),
                new LimitClause(new Constant(0), new Constant(10))
        );
        assertEquals(expected, actual);
    }

    @Test
    public final void testAnalyzeSelectStatement_UnsupportedAdditionExpression() throws SQLException {
        final String sql = "select id, name, score_english + score_chinese as total_score from student limit 0, 9";
        try {
            final Statement actual = GrammarAnalyzer.analyze(sql);
            final Statement expected = new SelectStatement(
                    new SelectClause(
                            null,
                            null,
                            new SelectClause.ResultColumnDeclare(
                                    (String) null,
                                    new Reference(null, "id")
                            ),
                            new SelectClause.ResultColumnDeclare(
                                    (String) null,
                                    new Reference(null, "name")
                            ),
                            new SelectClause.ResultColumnDeclare(
                                    "total_score",
                                    new AdditionExpression(
                                            new Reference(null, "score_english"),
                                            new Reference(null, "score_chinese")
                                    )
                            )
                    ),
                    new FromClause(new FromClause.SimpleTableDeclare(
                            null,
                            "student"
                    )),
                    null,
                    null,
                    null,
                    null,
                    new LimitClause(new Constant(0), new Constant(9))
            );
            assertFalse(expected.equals(actual));
        } catch (SQLException e) {
            assertTrue(true);
        }
    }

    @Test
    public final void testAnalyzeSelectStatement_UnsupportedDivisionExpression() {
        final String sql = "select name, (score_english + score_chinese) / 2 from student";
        try {
            final Statement actual = GrammarAnalyzer.analyze(sql);
            final Statement expected = new SelectStatement(
                    new SelectClause(
                            null,
                            null,
                            new SelectClause.ResultColumnDeclare(
                                    (String) null,
                                    new Reference(null, "name")
                            ),
                            new SelectClause.ResultColumnDeclare(
                                    (String) null,
                                    new DivisionExpression(
                                            new AdditionExpression(
                                                    new Reference(null, "score_english"),
                                                    new Reference(null, "score_chinese")
                                            ),
                                            new Constant(2)
                                    )
                            )
                    ),
                    new FromClause(new FromClause.SimpleTableDeclare(
                            null,
                            "student"
                    )),
                    null,
                    null,
                    null,
                    null,
                    null
            );
            assertFalse(expected.equals(actual));
        } catch (SQLException e) {
            assertTrue(true);
        }
    }

    @Test
    public final void testAnalyzeSelectStatement_Error() throws Exception {
        final String sql = "select * from student where 1 > 2 and 1";
        Statement statement = GrammarAnalyzer.analyze(sql);
        System.out.println(statement);
    }

}