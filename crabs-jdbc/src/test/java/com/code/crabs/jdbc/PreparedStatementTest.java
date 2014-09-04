package com.code.crabs.jdbc;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PreparedStatementTest extends StatementTestBase {

    @Test
    public void testExecuteQuery_OK() throws Exception {
        final String sql = "select stuno, stuname from student where stuno = ?";
        final PreparedStatement pstmt = this.connection.prepareStatement(sql);
        try {
            pstmt.setLong(1, 10011L);
            final ResultSet rs = pstmt.executeQuery();
            int rowCount = 0;
            try {
                while (rs.next()) {
                    rowCount++;
                    assertEquals("lisi", rs.getString(2));
                }
                assertEquals(1, rowCount);
            } finally {
                rs.close();
            }
        } finally {
            pstmt.close();
        }
    }

    @Test
    public void testExecuteQuery_OK_Date() throws Exception {
        final String sql = "select birthday, stuname from student where birthday = ?";
        final PreparedStatement pstmt = this.connection.prepareStatement(sql);
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            pstmt.setObject(1, dateFormat.parse("1999-02-28 00:00:00"));
            final ResultSet rs = pstmt.executeQuery();
            int rowCount = 0;
            try {
                while (rs.next()) {
                    rowCount++;
                    assertEquals("lisi", rs.getString(2));
                }
                assertEquals(1, rowCount);
            } finally {
                rs.close();
            }
        } finally {
            pstmt.close();
        }
    }

    @Test
    public void testExecuteQuery_Error_NoAssignedArgumentValues() throws Exception {
        final String sql = "select stuno, stuname from student where stuno = ?";
        final PreparedStatement pstmt = this.connection.prepareStatement(sql);
        try {
            pstmt.executeQuery();
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
        } finally {
            pstmt.close();
        }
    }

    @Test
    public void testExecuteQuery_Error_AssginedNullToArgument() throws Exception {
        final String sql = "select stuno, stuname from student where stuno = ?";
        final PreparedStatement pstmt = this.connection.prepareStatement(sql);
        try {
            pstmt.setObject(1, null);
            pstmt.executeQuery();
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
        } finally {
            pstmt.close();
        }
    }

}