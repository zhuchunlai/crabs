package org.codefamily.crabs.jdbc;

import org.junit.After;
import org.junit.Before;

import java.sql.*;

/**
 * @author zhuchunlai
 * @version $Id: StatementTestBase.java, v1.0 2014/08/29 15:42 $
 */
abstract class StatementTestBase {

    private static final String URL = "jdbc:elasticsearch://localhost:9300/test";

    protected java.sql.Connection connection;

    @Before
    public void setUp() throws Exception {
        this.connection = DriverManager.getConnection(URL);
    }

    @After
    public void tearDown() throws Exception {
        this.connection.close();
    }

}
