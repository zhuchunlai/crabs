package com.code.crabs.jdbc;

import com.code.crabs.exception.crabsException;
import com.code.crabs.jdbc.BaseClasses.StatementBase;
import com.code.crabs.jdbc.engine.ExecuteEngine;
import com.code.crabs.jdbc.internal.InternalResultSet;
import com.code.crabs.jdbc.lang.extension.statement.SelectStatement;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;

public class Statement extends StatementBase {

    public static final int NO_UPDATE_COUNT = -1;

    Statement(final Connection connection) {
        this.connection = connection;
        this.lastUpdateCount = NO_UPDATE_COUNT;
        this.closed = false;
    }

    private final Connection connection;

    private int maxinumRowCount;

    private ResultSet lastResultSet;

    private int lastUpdateCount;

    private boolean closed;

    @Override
    public final Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public final int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public final int getMaxRows() throws SQLException {
        return this.maxinumRowCount;
    }

    @Override
    public final int getQueryTimeout() throws SQLException {
        // todo
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public final int getUpdateCount() throws SQLException {
        final int lastUpdateCount = this.lastUpdateCount;
        this.lastUpdateCount = NO_UPDATE_COUNT;
        return lastUpdateCount;
    }

    @Override
    public final boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public final boolean getMoreResults(final int current) throws SQLException {
        return false;
    }

    @Override
    public final int getFetchSize() throws SQLException {
        return Integer.parseInt(
                this.connection.executeEnvironment.getProperty(
                        Protocol.PROPERTY_ENTRY$SCAN_SIZE.identifier,
                        Protocol.PROPERTY_ENTRY$SCAN_SIZE.defaultValue
                )
        );
    }

    @Override
    public final int getFetchDirection() throws SQLException {
        final ResultSet resultSet = this.lastResultSet;
        if (resultSet == null) {
            return Protocol.RESULT_SET_FETCH_DIRECTION;
        } else {
            return resultSet.getFetchDirection();
        }
    }

    @Override
    public final ResultSet getResultSet() throws SQLException {
        final ResultSet resultSet = this.lastResultSet;
        this.lastResultSet = null;
        return resultSet;
    }

    @Override
    public final int getResultSetType() throws SQLException {
        final ResultSet resultSet = this.lastResultSet;
        if (resultSet == null) {
            return Protocol.RESULT_SET_TYPE;
        } else {
            return resultSet.getType();
        }
    }

    @Override
    public final int getResultSetHoldability() throws SQLException {
        final ResultSet resultSet = this.lastResultSet;
        if (resultSet == null) {
            return Protocol.RESULT_SET_HOLDABILITY;
        } else {
            return resultSet.getHoldability();
        }
    }

    @Override
    public final int getResultSetConcurrency() throws SQLException {
        final ResultSet resultSet = this.lastResultSet;
        if (resultSet == null) {
            return Protocol.RESULT_SET_CONCURRENCY;
        } else {
            return resultSet.getConcurrency();
        }
    }

    @Override
    public final boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public final boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean isWrapperFor(final Class<?> iface) throws SQLException {
        if (iface == null) {
            throw new IllegalArgumentException("Argument [iface] is null.");
        }
        return iface.isInstance(this);
    }

    @Override
    public final void setMaxRows(final int maxinumRowCount) throws SQLException {
        this.maxinumRowCount = maxinumRowCount;
    }

    @Override
    public final void clearWarnings() throws SQLException {
        // to do nothing.
    }

    @Override
    public final boolean execute(final String SQL) throws SQLException {
        if (SQL == null) {
            throw new IllegalArgumentException("Argument [SQL] is null.");
        }
        final com.code.crabs.jdbc.lang.Statement statement = this.connection.analyzeStatement(SQL);
        if (statement instanceof SelectStatement) {
            this.executeQuery((SelectStatement) statement);
            return true;
        } else {
            this.executeUpdate(statement);
            return false;
        }
    }

    @Override
    public final ResultSet executeQuery(final String SQL) throws SQLException {
        if (SQL == null) {
            throw new IllegalArgumentException("Argument [SQL] is null.");
        }
        final com.code.crabs.jdbc.lang.Statement statement = this.connection.analyzeStatement(SQL);
        if (!(statement instanceof SelectStatement)) {
            throw new SQLException("SQL is not a query statement, detail as bellow: \n" + SQL);
        }
        return this.executeQuery((SelectStatement) statement);
    }

    @Override
    public final int executeUpdate(final String SQL) throws SQLException {
        if (SQL == null) {
            throw new IllegalArgumentException("Argument [SQL] is null.");
        }
        final com.code.crabs.jdbc.lang.Statement statement = this.connection.analyzeStatement(SQL);
        if (statement instanceof SelectStatement) {
            throw new SQLException("SQL is not a update statement, detail as bellow: \n" + SQL);
        } else {
            return this.executeUpdate(statement);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface == null) {
            throw new IllegalArgumentException("Argument [iface] is null.");
        }
        if (!iface.isInstance(this)) {
            throw new SQLException(this.getClass().getName() + " not unwrappable from " + iface.getName());
        }
        return (T) this;
    }

    @Override
    public void close() throws SQLException {
        try {
            this.lastResultSet = null;
        } finally {
            this.closed = true;
        }
    }

    final ResultSet executeQuery(final SelectStatement selectStatement,
                                 final Object... argumentValues) throws SQLException {
        final InternalResultSet resultSet;
        try {
            resultSet = ExecuteEngine.executeStatement(
                    this.connection.advancedClient,
                    this.connection.executeEnvironment,
                    selectStatement,
                    argumentValues,
                    InternalResultSet.class
            );
        } catch (crabsException e) {
            throw new SQLException(e.getMessage(), e);
        }
        return this.lastResultSet = new ResultSet(this.connection, this, resultSet);
    }

    final int executeUpdate(final com.code.crabs.jdbc.lang.Statement statement,
                            final Object... argumentValues) throws SQLException {
        try {
            return this.lastUpdateCount = ExecuteEngine.executeStatement(
                    this.connection.advancedClient,
                    this.connection.executeEnvironment,
                    statement,
                    argumentValues,
                    Integer.class
            );
        } catch (crabsException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    final ResultSet getLastResultSet() {
        return this.lastResultSet;
    }

}
