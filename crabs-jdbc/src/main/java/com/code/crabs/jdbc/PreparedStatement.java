package com.code.crabs.jdbc;

import com.code.crabs.exception.crabsException;
import com.code.crabs.jdbc.BaseClasses.PreparedStatementBase;
import com.code.crabs.jdbc.ResultSet.InternalResultSetMetaData;
import com.code.crabs.jdbc.lang.Statement;
import com.code.crabs.jdbc.lang.extension.statement.SelectStatement;

import java.sql.*;
import java.sql.Date;
import java.util.*;

public final class PreparedStatement extends PreparedStatementBase {

    private static int toInnerParameterIndex(final int outerParameterIndex) {
        return outerParameterIndex - 1;
    }

    private static void checkOuterParameterIndex(final int parameterIndex) {
        if (parameterIndex <= 0) {
            throw new IllegalArgumentException("Argument[parameterIndex] must be greater than 0.");
        }
    }

    private static final Object[] EMPTY_ARGUMENT_VALUES = new Object[0];

    PreparedStatement(final Connection connection, final String SQL) throws SQLException {
        super(connection);
        this.SQL = SQL;
        this.statement = connection.analyzeStatement(SQL);
    }

    final String SQL;

    final Statement statement;

    private InnerParameterMetaData innerParameterMetaData;

    private ArrayList<Object> parameterValueList;

    @Override
    public final InternalResultSetMetaData getMetaData() throws SQLException {
        final ResultSet resultSet = this.getLastResultSet();
        return resultSet == null ? null : resultSet.getMetaData();
    }

    @Override
    public final InnerParameterMetaData getParameterMetaData() throws SQLException {
        if (this.innerParameterMetaData == null) {
            this.innerParameterMetaData = new InnerParameterMetaData(this);
        }
        return this.innerParameterMetaData;
    }

    @Override
    public final void setNull(final int parameterIndex, final int SQLType) throws SQLException {
        checkOuterParameterIndex(parameterIndex);
        this.ensureParameterValueList(parameterIndex);
        this.parameterValueList.set(toInnerParameterIndex(parameterIndex), null);
    }

    @Override
    public final void setNull(final int parameterIndex,
                              final int SQLType,
                              final String typeName) throws SQLException {
        checkOuterParameterIndex(parameterIndex);
        this.ensureParameterValueList(parameterIndex);
        this.parameterValueList.set(toInnerParameterIndex(parameterIndex), null);
    }

    @Override
    public final void setShort(final int parameterIndex, final short value) throws SQLException {
        checkOuterParameterIndex(parameterIndex);
        this.ensureParameterValueList(parameterIndex);
        this.parameterValueList.set(toInnerParameterIndex(parameterIndex), value);
    }

    @Override
    public final void setInt(final int parameterIndex, final int value) throws SQLException {
        checkOuterParameterIndex(parameterIndex);
        this.ensureParameterValueList(parameterIndex);
        this.parameterValueList.set(toInnerParameterIndex(parameterIndex), value);
    }

    @Override
    public final void setLong(final int parameterIndex, final long value) throws SQLException {
        checkOuterParameterIndex(parameterIndex);
        this.ensureParameterValueList(parameterIndex);
        this.parameterValueList.set(toInnerParameterIndex(parameterIndex), value);
    }

    @Override
    public final void setFloat(final int parameterIndex, final float value) throws SQLException {
        checkOuterParameterIndex(parameterIndex);
        this.ensureParameterValueList(parameterIndex);
        this.parameterValueList.set(toInnerParameterIndex(parameterIndex), value);
    }

    @Override
    public final void setDouble(final int parameterIndex, final double value) throws SQLException {
        checkOuterParameterIndex(parameterIndex);
        this.ensureParameterValueList(parameterIndex);
        this.parameterValueList.set(toInnerParameterIndex(parameterIndex), value);
    }

    @Override
    public final void setDate(final int parameterIndex,
                              final Date value) throws SQLException {
        checkOuterParameterIndex(parameterIndex);
        this.ensureParameterValueList(parameterIndex);
        this.parameterValueList.set(toInnerParameterIndex(parameterIndex), new java.util.Date(value.getTime()));
    }

    @Override
    public final void setString(final int parameterIndex, final String value) throws SQLException {
        checkOuterParameterIndex(parameterIndex);
        this.ensureParameterValueList(parameterIndex);
        this.parameterValueList.set(toInnerParameterIndex(parameterIndex), value);
    }

    @Override
    public final void setTimestamp(final int parameterIndex,
                                   final Timestamp value) throws SQLException {
        checkOuterParameterIndex(parameterIndex);
        this.ensureParameterValueList(parameterIndex);
        this.parameterValueList.set(toInnerParameterIndex(parameterIndex), new java.util.Date(value.getTime()));
    }

    @Override
    public final void setObject(final int parameterIndex, Object value) throws SQLException {
        checkOuterParameterIndex(parameterIndex);
        this.ensureParameterValueList(parameterIndex);
        if (value instanceof Date) {
            value = new java.util.Date(((Date) value).getTime());
        } else if (value instanceof Timestamp) {
            value = new java.util.Date(((Timestamp) value).getTime());
        }
        this.parameterValueList.set(toInnerParameterIndex(parameterIndex), value);
    }

    @Override
    public final boolean execute() throws SQLException {
        final Statement statement = this.statement;
        if (statement instanceof SelectStatement) {
            this.executeQuery((SelectStatement) statement, this.getParameterValues());
            return true;
        } else {
            this.executeUpdate(statement, this.getParameterValues());
            return false;
        }
    }

    @Override
    public final ResultSet executeQuery() throws SQLException {
        final Statement statement = this.statement;
        if (statement instanceof SelectStatement) {
            return this.executeQuery((SelectStatement) statement, this.getParameterValues());
        } else {
            throw new SQLException("SQL is not a query statement. \n" + this.SQL);
        }
    }

    @Override
    public final int executeUpdate() throws SQLException {
        final Statement statement = this.statement;
        if (statement instanceof SelectStatement) {
            throw new SQLException("SQL is not a update statement. \n" + this.SQL);
        } else {
            return this.executeUpdate(statement, this.getParameterValues());
        }
    }

    @Override
    public final void clearParameters() throws SQLException {
        if (this.parameterValueList != null) {
            this.parameterValueList.clear();
        }
    }

    @Override
    public final void close() throws SQLException {
        try {
            super.close();
        } finally {
            this.parameterValueList = null;
        }
    }

    private void ensureParameterValueList(final int expectCapacity) {
        if (this.parameterValueList == null) {
            this.parameterValueList = new ArrayList<Object>();
        }
        if (this.parameterValueList.size() < expectCapacity) {
            for (int i = this.parameterValueList.size(); i < expectCapacity; i++) {
                this.parameterValueList.add(null);
            }
        }
    }

    private Object[] getParameterValues() {
        final ArrayList<Object> parameterValueList = this.parameterValueList;
        if (parameterValueList == null || parameterValueList.size() == 0) {
            return EMPTY_ARGUMENT_VALUES;
        } else {
            return parameterValueList.toArray();
        }
    }

    static final class InnerParameterMetaData implements java.sql.ParameterMetaData {

        InnerParameterMetaData(final PreparedStatement preparedStatement) {
            this.preparedStatement = preparedStatement;
        }

        private final PreparedStatement preparedStatement;

        @Override
        public final int getParameterCount() throws SQLException {
            try {
                return preparedStatement.statement.getParameterCount();
            } catch (crabsException ex) {
                throw new SQLException(ex.getMessage(), ex);
            }
        }

        @Override
        public final int getPrecision(final int parameterIndex) throws SQLException {
            checkOuterParameterIndex(parameterIndex);
            return 0;
        }

        @Override
        public final int getScale(final int parameterIndex) throws SQLException {
            checkOuterParameterIndex(parameterIndex);
            return 0;
        }

        @Override
        public final int getParameterType(final int parameterIndex) throws SQLException {
            checkOuterParameterIndex(parameterIndex);
            return Types.OTHER;
        }

        @Override
        public final String getParameterTypeName(final int parameterIndex) throws SQLException {
            checkOuterParameterIndex(parameterIndex);
            return "OTHER";
        }

        @Override
        public final String getParameterClassName(final int parameterIndex) throws SQLException {
            checkOuterParameterIndex(parameterIndex);
            return Object.class.getName();
        }

        @Override
        public final int getParameterMode(final int parameterIndex) throws SQLException {
            checkOuterParameterIndex(parameterIndex);
            return parameterModeIn;
        }

        @Override
        public final int isNullable(final int parameterIndex) throws SQLException {
            checkOuterParameterIndex(parameterIndex);
            return parameterNullableUnknown;
        }

        @Override
        public final boolean isSigned(final int parameterIndex) throws SQLException {
            checkOuterParameterIndex(parameterIndex);
            return true;
        }

        @Override
        public final boolean isWrapperFor(final Class<?> iface) throws SQLException {
            return iface.isInstance(this);
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T unwrap(final Class<T> iface) throws SQLException {
            if (iface == null) {
                throw new IllegalArgumentException("Argument[iface] is null.");
            }
            if (!iface.isInstance(this)) {
                throw new SQLException(this.getClass().getName()
                        + " not unwrappable from " + iface.getName());
            }
            return (T) this;
        }

    }

}
