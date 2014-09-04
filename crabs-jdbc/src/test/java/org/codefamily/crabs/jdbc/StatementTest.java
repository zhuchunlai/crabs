package org.codefamily.crabs.jdbc;

import org.junit.Test;

import java.sql.Statement;
import java.sql.ResultSet;

import static org.junit.Assert.*;

public class StatementTest extends StatementTestBase {

    @Test
    public void testExecuteQuery() throws Exception {
        final String sql = "select stuno, stuname from student where stuno = 10011";
        final Statement statement = this.connection.createStatement();
        try {
            final ResultSet resultSet = statement.executeQuery(sql);
            int rowCount = 0;
            while (resultSet.next()) {
                rowCount++;
                assertEquals(10011L, resultSet.getLong(1));
                assertEquals("lisi", resultSet.getString(2));
            }
            assertEquals(1, rowCount);
            try {
            } finally {
                resultSet.close();
            }
        } finally {
            statement.close();
        }
    }
}