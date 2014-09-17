package org.codefamily.crabs.jdbc.engine.extension;

import org.codefamily.crabs.common.Constants;
import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.core.Identifier;
import org.codefamily.crabs.core.IndexDefinition;
import org.codefamily.crabs.core.TypeDefinition;
import org.codefamily.crabs.core.client.AdvancedClient;
import org.codefamily.crabs.exception.SQL4ESException;
import org.codefamily.crabs.jdbc.compiler.GrammarAnalyzer;
import org.codefamily.crabs.jdbc.engine.ExecuteEngine;
import org.codefamily.crabs.jdbc.engine.ExecuteEnvironment;
import org.codefamily.crabs.jdbc.internal.InternalResultSet;
import org.codefamily.crabs.jdbc.internal.InternalResultSet.InternalMetaData;
import org.codefamily.crabs.jdbc.lang.extension.statement.SelectStatement;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.codefamily.crabs.common.Constants.PATTERN_YYYY_MM_DD_HH_MM_SS;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        SelectStatementExecutorTest.NonAggregationNormalSearchExecutorTest.class,
        SelectStatementExecutorTest.AggregationNormalSearchExecutorTest.class
})
public class SelectStatementExecutorTest {

    protected static final String HOST = "127.0.0.1";

    protected final AdvancedClient advancedClient;

    protected final ExecuteEnvironment environment;

    protected final String sql;

    protected final InternalMetaData expectedMetaData;

    protected final Object[][] expectedValues;

    public SelectStatementExecutorTest(final String sql,
                                       final InternalMetaData expectedMetaData,
                                       final Object[][] expectedValues) {
        this.advancedClient = new AdvancedClient(
                new AdvancedClient.ElasticsearchAddress[]{
                        new AdvancedClient.ElasticsearchAddress(HOST, 9300)
                }
        );
        this.environment = new ExecuteEnvironment(advancedClient, new Identifier("test"));
        this.sql = sql;
        this.expectedMetaData = expectedMetaData;
        this.expectedValues = expectedValues;
    }

    @Test
    public void testExecute() throws Exception {
        final SelectStatement statement = (SelectStatement) GrammarAnalyzer.analyze(this.sql);
        final InternalResultSet actualResultSet = ExecuteEngine.executeStatement(
                this.advancedClient,
                this.environment,
                statement,
                new Object[0],
                InternalResultSet.class
        );
        final InternalMetaData actualMetaData = actualResultSet.getMetaData();
        assertEquals(this.expectedMetaData, actualMetaData);
        final int columnCount = this.expectedMetaData.getColumnCount();
        int rowCount = 0, rowIndex = -1;
        Object expectedValue;
        while (actualResultSet.next()) {
            rowCount++;
            rowIndex++;
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                expectedValue = this.expectedValues[rowIndex][columnIndex];
                assertEquals(expectedValue, actualResultSet.getColumnValue(columnIndex));
                assertEquals(expectedValue, actualResultSet.getColumnValue(actualMetaData.getColumnIdentifier(columnIndex)));
            }
        }
        assertEquals(this.expectedValues.length, rowCount);
    }

    @After
    public void tearDown() throws Exception {
        this.advancedClient.close();
    }

    @RunWith(Parameterized.class)
    public static final class NonAggregationNormalSearchExecutorTest extends SelectStatementExecutorTest {

        public NonAggregationNormalSearchExecutorTest(final String sql,
                                                      final InternalMetaData expectedMetaData,
                                                      final Object[][] expectedValues) {
            super(sql, expectedMetaData, expectedValues);
        }

        static {
            final AdvancedClient advancedClient = new AdvancedClient(
                    new AdvancedClient.ElasticsearchAddress[]{
                            new AdvancedClient.ElasticsearchAddress(HOST, 9300)
                    }
            );
            try {
                init(advancedClient);
            } catch (SQL4ESException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    advancedClient.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Parameterized.Parameters
        public static Collection prepareData() throws Exception {
            final SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.PATTERN_YYYY_MM_DD_HH_MM_SS);
            final Object[][] data = {
                    {
                            "SELECT * FROM student order by stuno",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{dateFormat.parse("1999-01-30 00:00:00"), 95.5D, 97.5D, 101, "zhangsan", 10010L},
                                    new Object[]{dateFormat.parse("1999-02-28 00:00:00"), 92.5D, 99.5D, 102, "lisi", 10011L},
                                    new Object[]{dateFormat.parse("1999-03-30 00:00:00"), 95.5D, 90.5D, 101, "wangwu", 10012L},
                                    new Object[]{dateFormat.parse("2013-01-01 00:00:00"), 100.0D, null, 103, null, 10013L}

                            }
                    },
                    {
                            "SELECT * FROM student where chinesescore > 95 order by stuno",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{dateFormat.parse("1999-01-30 00:00:00"), 95.5D, 97.5D, 101, "zhangsan", 10010L},
                                    new Object[]{dateFormat.parse("1999-03-30 00:00:00"), 95.5D, 90.5D, 101, "wangwu", 10012L},
                                    new Object[]{dateFormat.parse("2013-01-01 00:00:00"), 100.0D, null, 103, null, 10013L}

                            }
                    },
                    {
                            "SELECT * FROM student where chinesescore > 95 and englishscore < 95 order by stuno",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{dateFormat.parse("1999-03-30 00:00:00"), 95.5D, 90.5D, 101, "wangwu", 10012L}

                            }
                    },
                    {
                            "SELECT * FROM student where chinesescore > 95 and stuclass in(102) order by stuno",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize())
                                    }
                            ),
                            new Object[][]{}
                    },
                    {
                            "SELECT * FROM student where chinesescore > 95 and stuname like '%an%' order by stuno",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{dateFormat.parse("1999-01-30 00:00:00"), 95.5D, 97.5D, 101, "zhangsan", 10010L},
                                    new Object[]{dateFormat.parse("1999-03-30 00:00:00"), 95.5D, 90.5D, 101, "wangwu", 10012L}
                            }
                    },
                    {
                            "SELECT * FROM student where chinesescore > 95 and stuname not like '%an%' order by stuno",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{dateFormat.parse("2013-01-01 00:00:00"), 100.0D, null, 103, null, 10013L}
                            }
                    },
                    {
                            "SELECT * FROM student where chinesescore > 95 and (stuname not like '%an%') order by stuno",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{dateFormat.parse("2013-01-01 00:00:00"), 100.0D, null, 103, null, 10013L}
                            }
                    },
                    {
                            "SELECT * FROM student where chinesescore > 95 and (stuname not like '%an%') or stuclass = 101  order by stuno",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{dateFormat.parse("1999-01-30 00:00:00"), 95.5D, 97.5D, 101, "zhangsan", 10010L},
                                    new Object[]{dateFormat.parse("1999-03-30 00:00:00"), 95.5D, 90.5D, 101, "wangwu", 10012L},
                                    new Object[]{dateFormat.parse("2013-01-01 00:00:00"), 100.0D, null, 103, null, 10013L}
                            }
                    },
                    {
                            "SELECT * FROM student where (chinesescore > 95 and (stuname not like '%an%') or stuclass = 101) and englishscore > 95   order by stuno",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{dateFormat.parse("1999-01-30 00:00:00"), 95.5D, 97.5D, 101, "zhangsan", 10010L}
                            }
                    },
                    {
                            "SELECT * FROM student order by stuno limit 0, 2",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{dateFormat.parse("1999-01-30 00:00:00"), 95.5D, 97.5D, 101, "zhangsan", 10010L},
                                    new Object[]{dateFormat.parse("1999-02-28 00:00:00"), 92.5D, 99.5D, 102, "lisi", 10011L}
                            }
                    },
                    {
                            "SELECT * FROM student where stuclass = 101 having chinesescore > 93 order by englishscore desc",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{dateFormat.parse("1999-01-30 00:00:00"), 95.5D, 97.5D, 101, "zhangsan", 10010L},
                                    new Object[]{dateFormat.parse("1999-03-30 00:00:00"), 95.5D, 90.5D, 101, "wangwu", 10012L}
                            }
                    },
                    {
                            "SELECT * FROM student order by stuclass, englishscore desc",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{dateFormat.parse("1999-01-30 00:00:00"), 95.5D, 97.5D, 101, "zhangsan", 10010L},
                                    new Object[]{dateFormat.parse("1999-03-30 00:00:00"), 95.5D, 90.5D, 101, "wangwu", 10012L},
                                    new Object[]{dateFormat.parse("1999-02-28 00:00:00"), 92.5D, 99.5D, 102, "lisi", 10011L},
                                    new Object[]{dateFormat.parse("2013-01-01 00:00:00"), 100.0D, null, 103, null, 10013L}
                            }
                    },
                    {
                            "SELECT *, stuno as id, \"test\" FROM student order by stuclass, englishscore desc",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("chinesescore"), "chinesescore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("englishscore"), "englishscore", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuname"), "stuname", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("id"), "id", DataType.LONG, DataType.LONG.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("test"), "test", DataType.STRING, DataType.STRING.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{dateFormat.parse("1999-01-30 00:00:00"), 95.5D, 97.5D, 101, "zhangsan", 10010L, 10010L, "test"},
                                    new Object[]{dateFormat.parse("1999-03-30 00:00:00"), 95.5D, 90.5D, 101, "wangwu", 10012L, 10012L, "test"},
                                    new Object[]{dateFormat.parse("1999-02-28 00:00:00"), 92.5D, 99.5D, 102, "lisi", 10011L, 10011L, "test"},
                                    new Object[]{dateFormat.parse("2013-01-01 00:00:00"), 100.0D, null, 103, null, 10013L, 10013L, "test"}
                            }
                    },
                    {
                            "SELECT stuno, birthday from student where birthday > '1999-03-01 00:00:00' order by stuno",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuno"), "stuno", DataType.LONG, DataType.LONG.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("birthday"), "birthday", DataType.DATE, DataType.DATE.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{10012L, dateFormat.parse("1999-03-30 00:00:00")},
                                    new Object[]{10013L, dateFormat.parse("2013-01-01 00:00:00")}
                            }
                    }
            };
            return Arrays.asList(data);
        }

        private static void init(final AdvancedClient advancedClient) throws SQL4ESException {
            final IndexDefinition indexDefinition = new IndexDefinition(new Identifier("test"));
            if (!advancedClient.existsIndex(indexDefinition.getIdentifier())) {
                advancedClient.createIndex(indexDefinition);
            }
            final TypeDefinition student = new TypeDefinition(indexDefinition, new Identifier("student"));
            student.defineLongField(new Identifier("stuno")).asPrimaryField();
            student.defineStringField(new Identifier("stuname"));
            student.defineIntegerField(new Identifier("stuclass"));
            student.defineDateField(new Identifier("birthday"), PATTERN_YYYY_MM_DD_HH_MM_SS);
            student.defineDoubleField(new Identifier("chinesescore"));
            student.defineDoubleField(new Identifier("englishscore"));
            student.publish();
            if (!advancedClient.existsType(student)) {
                advancedClient.createType(student);
            }

        }


    }

    @RunWith(Parameterized.class)
    public static final class AggregationNormalSearchExecutorTest extends SelectStatementExecutorTest {

        public AggregationNormalSearchExecutorTest(final String sql,
                                                   final InternalMetaData expectedMetaData,
                                                   final Object[][] expectedValues) {
            super(sql, expectedMetaData, expectedValues);
        }

        @Parameterized.Parameters
        public static Collection prepareData() throws Exception {
            final Object[][] data = {
                    {
                            "SELECT sum(chinesescore), avg(englishscore), count(*), max(chinesescore), min(englishscore) FROM student",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("SUM(chinesescore)"), "SUM(chinesescore)", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("AVG(englishscore)"), "AVG(englishscore)", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("COUNT(*)"), "COUNT(*)", DataType.LONG, DataType.LONG.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("MAX(chinesescore)"), "MAX(chinesescore)", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("MIN(englishscore)"), "MIN(englishscore)", DataType.DOUBLE, DataType.DOUBLE.displaySize())
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{383.5D, 95.83333333333333D, 4L, 100D, 90.5D}
                            }
                    },
                    {
                            "SELECT stuclass, sum(chinesescore), avg(englishscore) FROM student group by stuclass",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("SUM(chinesescore)"), "SUM(chinesescore)", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("AVG(englishscore)"), "AVG(englishscore)", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{101, 191D, 94D},
                                    new Object[]{102, 92.5D, 99.5D},
                                    new Object[]{103, 100D, Double.NaN}
                            }
                    },
                    {
                            "SELECT student.stuclass, sum(chinesescore), stuno stuNo, avg(englishscore) FROM student group by stuclass, stuno",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("SUM(chinesescore)"), "SUM(chinesescore)", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuNo"), "stuNo", DataType.LONG, DataType.LONG.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("AVG(englishscore)"), "AVG(englishscore)", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{101, 95.5D, 10010L, 97.5D},
                                    new Object[]{101, 95.5D, 10012L, 90.5D},
                                    new Object[]{102, 92.5D, 10011L, 99.5D},
                                    new Object[]{103, 100D, 10013L, Double.NaN}
                            }
                    },
                    {
                            "SELECT student.stuclass, sum(chinesescore), stuname stuName, avg(englishscore) FROM student group by stuclass, stuname",
                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData(
                                    new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation[]{
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuclass"), "stuclass", DataType.INTEGER, DataType.INTEGER.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("SUM(chinesescore)"), "SUM(chinesescore)", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("stuName"), "stuName", DataType.STRING, DataType.STRING.displaySize()),
                                            new SelectStatementExecutor.SearchResultSet.SearchResultSetMetaData.ColumnInformation(new Identifier("AVG(englishscore)"), "AVG(englishscore)", DataType.DOUBLE, DataType.DOUBLE.displaySize()),
                                    }
                            ),
                            new Object[][]{
                                    new Object[]{101, 95.5D, "wangwu", 90.5D},
                                    new Object[]{101, 95.5D, "zhangsan", 97.5D},
                                    new Object[]{102, 92.5D, "lisi", 99.5D}
                            }
                    }
            };
            return Arrays.asList(data);
        }

    }

}