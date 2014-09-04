package com.code.crabs.jdbc;

import com.code.crabs.common.Constants;
import com.code.crabs.core.DataType;
import com.code.crabs.core.Identifier;
import com.code.crabs.exception.SQL4ESException;
import com.code.crabs.jdbc.BaseClasses.ResultSetBase;
import com.code.crabs.jdbc.internal.InternalResultSet;
import com.code.crabs.jdbc.internal.InternalResultSet.InternalMetaData;
import com.code.crabs.jdbc.Connection.DatabaseTypeSystem;

import java.io.IOException;
import java.sql.*;
import java.sql.Date;

public final class ResultSet extends ResultSetBase {

    private static int toInnerColumnIndex(final int outerColumnIndex) {
        return outerColumnIndex - 1;
    }

    private static int toOuterColumnIndex(final int innerColumnIndex) {
        return innerColumnIndex + 1;
    }

    private static void checkOuterColumnIndex(final int columnIndex) {
        if (columnIndex <= 0) {
            throw new IllegalArgumentException("Argument [columnIndex] must be greater than 0.");
        }
    }

    ResultSet(final Connection connection,
              final Statement statement,
              final InternalResultSet resultSet) throws SQLException {
        this.resultSet = resultSet;
        this.connection = connection;
        this.statement = statement;
        this.rowIndex = -1;
        this.closed = false;
    }

    private final InternalResultSet resultSet;

    private final Connection connection;

    private final Statement statement;

    private InternalResultSetMetaData internalResultSetMetaData;

    private Integer rowIndex;

    private boolean wasNull;

    private boolean closed;

    @Override
    public final Statement getStatement() throws SQLException {
        return this.statement;
    }

    @Override
    public final InternalResultSetMetaData getMetaData() throws SQLException {
        if (this.internalResultSetMetaData == null) {
            this.internalResultSetMetaData = new InternalResultSetMetaData(
                    this.connection.getTypeSystem(),
                    this.resultSet.getMetaData()
            );
        }
        return this.internalResultSetMetaData;
    }

    @Override
    public final int findColumn(final String columnLabel) throws SQLException {
        if (columnLabel == null) {
            throw new IllegalArgumentException(
                    "Argument [columnLabel] is null.");
        }
        return this.getMetaData().getOuterColumnIndex(columnLabel);
    }

    @Override
    public final boolean isBeforeFirst() throws SQLException {
        return this.rowIndex != null && this.rowIndex < 0;
    }

    @Override
    public final boolean isAfterLast() throws SQLException {
        return this.rowIndex == null;
    }

    @Override
    public final boolean next() throws SQLException {
        try {
            if (this.resultSet.next()) {
                return true;
            } else {
                this.rowIndex = null;
                return false;
            }
        } catch (SQL4ESException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    @Override
    public final int getType() throws SQLException {
        return Protocol.RESULT_SET_TYPE;
    }

    @Override
    public final boolean wasNull() throws SQLException {
        return this.wasNull;
    }

    @Override
    public final boolean getBoolean(final int columnIndex) throws SQLException {
        final Object value = this.getObject(columnIndex);
        if (value != null) {
            final DataType columnDataType = this.getMetaData().getColumnDataType(columnIndex);
            switch (columnDataType == null ? DataType.getDataType(value.getClass()) : columnDataType) {
                case BOOLEAN:
                    return (Boolean) value;
                case INTEGER:
                    return ((Integer) value) == Constants.BOOLEAN_TRUE_BYTE;
                case LONG:
                    return ((Long) value) == Constants.BOOLEAN_TRUE_BYTE;
                case DOUBLE:
                    return ((Double) value) == Constants.BOOLEAN_TRUE_BYTE;
                case FLOAT:
                    return ((Float) value) == Constants.BOOLEAN_TRUE_BYTE;
                case STRING:
                    try {
                        return Boolean.parseBoolean((String) value);
                    } catch (ClassCastException e) {
                        throw new SQLException("Can not cast " + value.getClass().getName()
                                + " value to " + boolean.class.getName() + " object.");
                    }
            }
        } else {
            return false;
        }
        throw new SQLException("Can not cast " + value.getClass().getName()
                + " value to " + short.class.getName() + " object.");
    }

    @Override
    public final boolean getBoolean(final String columnLabel)
            throws SQLException {
        return this.getBoolean(this.findColumn(columnLabel));
    }

    @Override
    public final byte getByte(final int columnIndex) throws SQLException {
        final Object value = this.getObject(columnIndex);
        if (value != null) {
            final DataType columnValueType = this.getMetaData()
                    .getColumnDataType(columnIndex);
            switch (columnValueType == null ? DataType.getDataType(value.getClass()) : columnValueType) {
                case BOOLEAN:
                    return ((Boolean) value) ? Constants.BOOLEAN_TRUE_BYTE : Constants.BOOLEAN_FALSE_BYTE;
                case INTEGER:
                    return ((Integer) value).byteValue();
                case LONG:
                    return ((Long) value).byteValue();
                case DATE:
                    return Long.valueOf(((java.util.Date) value).getTime()).byteValue();
                case DOUBLE:
                    return ((Double) value).byteValue();
                case FLOAT:
                    return ((Float) value).byteValue();
            }
        } else {
            return 0;
        }
        throw new SQLException("Can not cast " + value.getClass().getName() +
                " value to " + byte.class.getName() + " object.");
    }

    @Override
    public final byte getByte(final String columnLabel) throws SQLException {
        return this.getByte(this.findColumn(columnLabel));
    }

    @Override
    public final short getShort(final int columnIndex) throws SQLException {
        final Object value = this.getObject(columnIndex);
        if (value != null) {
            final DataType columnValueType = this.getMetaData().getColumnDataType(columnIndex);
            switch (columnValueType == null ? DataType.getDataType(value.getClass()) : columnValueType) {
                case BOOLEAN:
                    return ((Boolean) value) ? Constants.BOOLEAN_TRUE_BYTE : Constants.BOOLEAN_FALSE_BYTE;
                case INTEGER:
                    return ((Integer) value).shortValue();
                case LONG:
                    return ((Long) value).shortValue();
                case DOUBLE:
                    return ((Double) value).shortValue();
                case FLOAT:
                    return ((Float) value).shortValue();
                case DATE:
                    return Long.valueOf(((java.util.Date) value).getTime()).shortValue();
            }
        } else {
            return 0;
        }
        throw new SQLException("Can not cast " + value.getClass().getName()
                + " value to " + short.class.getName() + " object.");
    }

    @Override
    public final short getShort(final String columnLabel) throws SQLException {
        return this.getShort(this.findColumn(columnLabel));
    }

    @Override
    public final int getInt(final int columnIndex) throws SQLException {
        final Object value = this.getObject(columnIndex);
        if (value != null) {
            final DataType columnValueType = this.getMetaData().getColumnDataType(columnIndex);
            switch (columnValueType == null ? DataType.getDataType(value.getClass()) : columnValueType) {
                case BOOLEAN:
                    return ((Boolean) value) ? Constants.BOOLEAN_TRUE_BYTE : Constants.BOOLEAN_FALSE_BYTE;
                case INTEGER:
                    return (Integer) value;
                case LONG:
                    return ((Long) value).intValue();
                case DOUBLE:
                    return ((Double) value).intValue();
                case FLOAT:
                    return ((Float) value).intValue();
                case DATE:
                    return Long.valueOf(((java.util.Date) value).getTime()).intValue();
                case STRING:
                    try {
                        return Integer.parseInt((String) value);
                    } catch (Exception e) {
                        throw new SQLException("Can not cast " + value.getClass().getName()
                                + " value to " + int.class.getName() + " object.");
                    }
            }
        } else {
            return 0;
        }
        throw new SQLException("Can not cast " + value.getClass().getName()
                + " value to " + int.class.getName() + " object.");
    }

    @Override
    public final int getInt(final String columnLabel) throws SQLException {
        return this.getInt(this.findColumn(columnLabel));
    }

    @Override
    public final long getLong(final int columnIndex) throws SQLException {
        final Object value = this.getObject(columnIndex);
        if (value != null) {
            final DataType columnValueType = this.getMetaData().getColumnDataType(columnIndex);
            switch (columnValueType == null ? DataType.getDataType(value.getClass()) : columnValueType) {
                case BOOLEAN:
                    return ((Boolean) value) ? Constants.BOOLEAN_TRUE_BYTE : Constants.BOOLEAN_FALSE_BYTE;
                case INTEGER:
                    return ((Integer) value).longValue();
                case LONG:
                    return (Long) value;
                case DOUBLE:
                    return ((Double) value).longValue();
                case FLOAT:
                    return ((Float) value).longValue();
                case DATE:
                    return ((java.util.Date) value).getTime();
                case STRING:
                    try {
                        return Long.parseLong((String) value);
                    } catch (Exception e) {
                        throw new SQLException("Can not cast " + value.getClass().getName()
                                + " value to " + long.class.getName() + " object.");
                    }
            }
        } else {
            return 0;
        }
        throw new SQLException("Can not cast " + value.getClass().getName()
                + " value to " + long.class.getName() + " object.");
    }

    @Override
    public final long getLong(final String columnLabel) throws SQLException {
        return this.getLong(this.findColumn(columnLabel));
    }

    @Override
    public final Timestamp getTimestamp(final int columnIndex) throws SQLException {
        final Object value = this.getObject(columnIndex);
        if (value != null) {
            final DataType columnValueType = this.getMetaData().getColumnDataType(columnIndex);
            switch (columnValueType == null ? DataType.getDataType(value.getClass()) : columnValueType) {
                case DATE:
                    return new Timestamp(((java.util.Date) value).getTime());
            }
        } else {
            return null;
        }
        throw new SQLException("Can not cast " + value.getClass().getName()
                + " value to " + Timestamp.class.getName() + " object.");
    }

    @Override
    public final Timestamp getTimestamp(final String columnLabel) throws SQLException {
        return this.getTimestamp(this.findColumn(columnLabel));
    }

    @Override
    public final Date getDate(final int columnIndex) throws SQLException {
        final Timestamp timestamp = this.getTimestamp(columnIndex);
        return timestamp == null ? null : new Date(timestamp.getTime());
    }

    @Override
    public final Date getDate(final String columnLabel) throws SQLException {
        return this.getDate(this.findColumn(columnLabel));
    }

    @Override
    public final float getFloat(final int columnIndex) throws SQLException {
        final Object value = this.getObject(columnIndex);
        if (value != null) {
            final DataType columnValueType = this.getMetaData().getColumnDataType(columnIndex);
            switch (columnValueType == null ? DataType.getDataType(value.getClass()) : columnValueType) {
                case BOOLEAN:
                    return ((Boolean) value) ? Constants.BOOLEAN_TRUE_BYTE : Constants.BOOLEAN_FALSE_BYTE;
                case INTEGER:
                    return ((Integer) value).floatValue();
                case LONG:
                    return ((Long) value).floatValue();
                case DOUBLE:
                    return ((Double) value).floatValue();
                case FLOAT:
                    return (Float) value;
                case DATE:
                    return Long.valueOf(((java.util.Date) value).getTime()).floatValue();
                case STRING:
                    try {
                        return Float.parseFloat((String) value);
                    } catch (Exception e) {
                        throw new SQLException("Can not cast " + value.getClass().getName()
                                + " value to " + float.class.getName() + " object.");
                    }
            }
        } else {
            return 0;
        }
        throw new SQLException("Can not cast " + value.getClass().getName()
                + " value to " + float.class.getName() + " object.");
    }

    @Override
    public final float getFloat(final String columnLabel) throws SQLException {
        return this.getFloat(this.findColumn(columnLabel));
    }

    @Override
    public final double getDouble(final int columnIndex) throws SQLException {
        final Object value = this.getObject(columnIndex);
        if (value != null) {
            final DataType columnValueType = this.getMetaData().getColumnDataType(columnIndex);
            switch (columnValueType == null ? DataType.getDataType(value.getClass()) : columnValueType) {
                case BOOLEAN:
                    return ((Boolean) value) ? Constants.BOOLEAN_TRUE_BYTE : Constants.BOOLEAN_FALSE_BYTE;
                case INTEGER:
                    return ((Integer) value).doubleValue();
                case LONG:
                    return ((Long) value).doubleValue();
                case DOUBLE:
                    return (Double) value;
                case FLOAT:
                    return ((Float) value).doubleValue();
                case DATE:
                    return Long.valueOf(((java.util.Date) value).getTime()).doubleValue();
                case STRING:
                    try {
                        return Double.parseDouble((String) value);
                    } catch (Exception e) {
                        throw new SQLException("Can not cast " + value.getClass().getName()
                                + " value to " + double.class.getName() + " object.");
                    }
            }
        } else {
            return 0;
        }
        throw new SQLException("Can not cast " + value.getClass().getName()
                + " value to " + double.class.getName() + " object.");
    }

    @Override
    public final double getDouble(final String columnLabel) throws SQLException {
        return this.getDouble(this.findColumn(columnLabel));
    }

    @Override
    public final Time getTime(final int columnIndex) throws SQLException {
        final Timestamp timestamp = this.getTimestamp(columnIndex);
        return timestamp == null ? null : new Time(timestamp.getTime());
    }

    @Override
    public final Time getTime(final String columnLabel) throws SQLException {
        return this.getTime(this.findColumn(columnLabel));
    }

    @Override
    public final String getString(final int columnIndex) throws SQLException {
        final Object value = this.getObject(columnIndex);
        return value == null ? null : value.toString();
    }

    @Override
    public final String getString(final String columnLabel) throws SQLException {
        return this.getString(this.findColumn(columnLabel));
    }

    @Override
    public final Object getObject(final int columnIndex) throws SQLException {
        checkOuterColumnIndex(columnIndex);
        final Object value = this.resultSet.getColumnValue(toInnerColumnIndex(columnIndex));
        this.wasNull = (value == null);
        return value;
    }

    @Override
    public final Object getObject(final String columnLabel) throws SQLException {
        return this.getObject(this.findColumn(columnLabel));
    }

    @Override
    public final int getHoldability() throws SQLException {
        return Protocol.RESULT_SET_HOLDABILITY;
    }

    @Override
    public final SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public final boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public final <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        final Object value = this.getObject(columnIndex);
        try {
            return type.cast(value);
        } catch (ClassCastException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override
    public final <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
        final Object value = this.getObject(columnLabel);
        try {
            return type.cast(value);
        } catch (ClassCastException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override
    public final void clearWarnings() throws SQLException {
        // to do nothing.
    }

    @Override
    public final int getFetchSize() throws SQLException {
        return this.statement.getFetchSize();
    }

    @Override
    public final int getFetchDirection() throws SQLException {
        return Protocol.RESULT_SET_FETCH_DIRECTION;
    }

    @Override
    public final int getConcurrency() throws SQLException {
        return Protocol.RESULT_SET_CONCURRENCY;
    }

    @Override
    public final boolean isWrapperFor(final Class<?> iface) throws SQLException {
        if (iface == null) {
            throw new IllegalArgumentException("Argument [iface] is null.");
        }
        return iface.isInstance(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface == null) {
            throw new IllegalArgumentException("Argument [iface] is null.");
        }
        if (!iface.isInstance(this)) {
            throw new SQLException(this.getClass().getName()
                    + " not unwrappable from " + iface.getName());
        }
        return (T) this;
    }

    @Override
    public final void close() throws SQLException {
        try {
            this.resultSet.close();
        } catch (IOException ex) {
            throw new SQLException(ex.getMessage(), ex);
        } finally {
            this.rowIndex = null;
            this.wasNull = false;
            this.closed = true;
        }
    }

    static final class InternalResultSetMetaData implements java.sql.ResultSetMetaData {

        InternalResultSetMetaData(final DatabaseTypeSystem databaseTypeSystem,
                                  final InternalMetaData metaData) {
            this.databaseTypeSystem = databaseTypeSystem;
            this.metaData = metaData;
        }

        private final DatabaseTypeSystem databaseTypeSystem;

        private final InternalMetaData metaData;

        @Override
        public final int getColumnCount() throws SQLException {
            return this.metaData.getColumnCount();
        }

        @Override
        public final String getSchemaName(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return "";
        }

        @Override
        public final String getCatalogName(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return "";
        }

        @Override
        public final String getTableName(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return "";
        }

        @Override
        public final int getColumnDisplaySize(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return this.metaData.getColumnDisplaySize(toInnerColumnIndex(columnIndex));
        }

        @Override
        public final String getColumnLabel(final int columnIndex)
                throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return this.metaData.getColumnLabel(toInnerColumnIndex(columnIndex));
        }

        @Override
        public final String getColumnName(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return this.metaData.getColumnIdentifier(toInnerColumnIndex(columnIndex)).toString();
        }

        @Override
        public final int getColumnType(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return this.databaseTypeSystem.getTypeCode(
                    this.metaData.getColumnValueType(toInnerColumnIndex(columnIndex))
            );
        }

        @Override
        public final String getColumnTypeName(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return this.databaseTypeSystem.getTypeName(
                    this.metaData.getColumnValueType(toInnerColumnIndex(columnIndex))
            );
        }

        @Override
        public final String getColumnClassName(final int columnIndex)
                throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return this.databaseTypeSystem
                    .getTypeClass(
                            this.metaData
                                    .getColumnValueType(toInnerColumnIndex(columnIndex)))
                    .getName();
        }

        @Override
        public final int getPrecision(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return 0;
        }

        @Override
        public final int getScale(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return 0;
        }

        @Override
        public final boolean isAutoIncrement(final int columnIndex)
                throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return false;
        }

        @Override
        public final boolean isCaseSensitive(final int columnIndex)
                throws SQLException {
            checkOuterColumnIndex(columnIndex);
            // 返回指定字段是否为大小写敏感字段
            return true;
        }

        @Override
        public final boolean isSearchable(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            final DataType columnDataType = this.metaData.getColumnValueType(toInnerColumnIndex(columnIndex));
            return columnDataType != null && this.databaseTypeSystem.isTypeSearchable(columnDataType);
        }

        @Override
        public final boolean isCurrency(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            // 返回指定字段是否为货币字段
            return false;
        }

        @Override
        public final int isNullable(final int columnIndex) throws SQLException {
            return ResultSetMetaData.columnNullableUnknown;
        }

        @Override
        public final boolean isSigned(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return true;
        }

        @Override
        public final boolean isReadOnly(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return true;
        }

        @Override
        public final boolean isWritable(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return false;
        }

        @Override
        public final boolean isDefinitelyWritable(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return false;
        }

        @Override
        public final boolean isWrapperFor(final Class<?> iface)
                throws SQLException {
            return iface.isInstance(this);
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T unwrap(final Class<T> iface) throws SQLException {
            if (iface == null) {
                throw new IllegalArgumentException("Argument[iface] is null.");
            }
            if (!iface.isInstance(this)) {
                throw new SQLException(this.getClass().getName() + " not unwrappable from " + iface.getName());
            }
            return (T) this;
        }

        final DataType getColumnDataType(final int columnIndex) throws SQLException {
            checkOuterColumnIndex(columnIndex);
            return this.metaData.getColumnValueType(
                    toInnerColumnIndex(columnIndex)
            );
        }

        final int getOuterColumnIndex(final String columnLabel) {
            return toOuterColumnIndex(
                    this.metaData.getColumnIndex(
                            new Identifier(columnLabel)
                    )
            );
        }

    }

}
