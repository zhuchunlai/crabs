package org.codefamily.crabs.jdbc;

import org.codefamily.crabs.Product;
import org.codefamily.crabs.util.ReadonlyList;
import org.codefamily.crabs.util.RegularExpressionHelper;
import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.core.Identifier;
import org.codefamily.crabs.core.TypeDefinition;
import org.codefamily.crabs.core.TypeDefinition.FieldDefinition;
import org.codefamily.crabs.core.client.AdvancedClient;
import org.codefamily.crabs.exception.CrabsException;
import org.codefamily.crabs.jdbc.BaseClasses.ConnectionBase;
import org.codefamily.crabs.jdbc.Protocol.PropertyEntry;
import org.codefamily.crabs.jdbc.compiler.GrammarAnalyzer;
import org.codefamily.crabs.jdbc.compiler.StatementCache;
import org.codefamily.crabs.jdbc.engine.ExecuteEnvironment;
import org.codefamily.crabs.jdbc.internal.InternalResultSet;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public final class Connection extends ConnectionBase {

    private static final int TRANSACTION_LEVEL = Connection.TRANSACTION_NONE;

    final AdvancedClient advancedClient;

    final ExecuteEnvironment executeEnvironment;

    private final String URL;

    private final Properties properties;

    private final Identifier connectedDatabaseIdentifier;

    private final StatementCache statementCache;

    Connection(final String URL, final Properties properties) throws SQLException {
        final Protocol protocol;
        try {
            protocol = Protocol.parseURL(URL);
        } catch (CrabsException e) {
            throw new SQLException(e.getMessage(), e);
        }
        final Properties finallyProperties = new Properties();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            finallyProperties.put(entry.getKey().toString(), entry.getValue().toString());
        }
        for (Map.Entry<Object, Object> entry : protocol.getProperties().entrySet()) {
            finallyProperties.setProperty(entry.getKey().toString(), entry.getValue().toString());
        }
        final Identifier connectedDatabaseIdentifier = new Identifier(protocol.getDatabaseName());
        final AdvancedClient advancedClient = new AdvancedClient(protocol.getServerAddresses(), finallyProperties);
        final boolean existsIndex;
        try {
            existsIndex = advancedClient.existsIndex(connectedDatabaseIdentifier);
        } catch (CrabsException ex) {
            try {
                advancedClient.close();
            } catch (IOException ioex) {
                throw new SQLException("Failed to initialize connection", ioex);
            }
            throw new SQLException(ex.getMessage(), ex);
        }
        if (!existsIndex) {
            try {
                advancedClient.close();
            } catch (IOException ex) {
                throw new SQLException("Failed to initialize connection", ex);
            }
            throw new SQLException("Database[" + protocol.getDatabaseName() + "] is not found.");
        }
        final StatementCache statementCache;
        try {
            statementCache = new StatementCache();
            statementCache.start();
        } catch (Throwable ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
        this.URL = URL;
        this.advancedClient = advancedClient;
        this.executeEnvironment
                = new ExecuteEnvironment(advancedClient, connectedDatabaseIdentifier, finallyProperties);
        this.properties = finallyProperties;
        this.connectedDatabaseIdentifier = connectedDatabaseIdentifier;
        this.statementCache = statementCache;
        this.autoCommit = false;
        this.closed = false;
    }

    private DatabaseTypeSystem databaseTypeSystem;

    private boolean autoCommit;

    private boolean closed;

    public final String getURL() {
        return this.URL;
    }

    public final DatabaseTypeSystem getTypeSystem() {
        if (this.databaseTypeSystem == null) {
            this.databaseTypeSystem = new DatabaseTypeSystem(this);
        }
        return this.databaseTypeSystem;
    }

    @Override
    public final DatabaseMetaData getMetaData() throws SQLException {
        return new DatabaseMetaData(this);
    }

    @Override
    public final boolean getAutoCommit() throws SQLException {
        return this.autoCommit;
    }

    @Override
    public final String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public final SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public final int getTransactionIsolation() throws SQLException {
        return TRANSACTION_LEVEL;
    }

    @Override
    public final Map<String, Class<?>> getTypeMap() throws SQLException {
        return this.getTypeSystem().getTypeMap();
    }

    @Override
    public final int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public final Properties getClientInfo() throws SQLException {
        return this.properties;
    }

    @Override
    public final void setSchema(final String schema) throws SQLException {
        // nothing to do.
    }

    @Override
    public final String getSchema() throws SQLException {
        return this.connectedDatabaseIdentifier.toString();
    }

    @Override
    public final String getClientInfo(final String name) {
        return this.properties.getProperty(name);
    }

    @Override
    public final boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public final boolean isWrapperFor(final Class<?> iface) throws SQLException {
        if (iface == null) {
            throw new IllegalArgumentException("Argument [iface] is null.");
        }
        return iface.isInstance(this);
    }

    @Override
    public final boolean isValid(final int timeout) throws SQLException {
        return !this.isClosed();
    }

    @Override
    public final boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public final void setAutoCommit(final boolean autoCommit)
            throws SQLException {
        this.autoCommit = autoCommit;
    }

    @Override
    public final void setTransactionIsolation(final int level) throws SQLException {
        if (level != TRANSACTION_LEVEL) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public final Statement createStatement() throws SQLException {
        return new Statement(this);
    }

    @Override
    public final Statement createStatement(final int resultSetType,
                                           final int resultSetConcurrency) throws SQLException {
        if (resultSetType != Protocol.RESULT_SET_TYPE
                || resultSetConcurrency != Protocol.RESULT_SET_CONCURRENCY) {
            throw new SQLFeatureNotSupportedException();
        } else {
            return createStatement();
        }
    }

    @Override
    public final Statement createStatement(final int resultSetType,
                                           final int resultSetConcurrency,
                                           final int resultSetHoldability) throws SQLException {
        if (resultSetHoldability != Protocol.RESULT_SET_HOLDABILITY) {
            throw new SQLFeatureNotSupportedException();
        } else {
            return createStatement(resultSetType, resultSetConcurrency);
        }
    }

    @Override
    public final java.sql.PreparedStatement prepareStatement(final String SQL) throws SQLException {
        if (SQL == null) {
            throw new IllegalArgumentException("Argument [SQL] is null.");
        }
        return new PreparedStatement(this, SQL);
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
    public final void clearWarnings() throws SQLException {
        // to do nothing.
    }

    @Override
    public final void commit() throws SQLException {
        // to do nothing.
    }

    @Override
    public final void rollback() throws SQLException {
        // to do nothing.
    }

    @Override
    public final void close() throws SQLException {
        if (this.closed) {
            return;
        }
        try {
            this.statementCache.stop();
        } finally {
            try {
                this.executeEnvironment.close();
            } catch (IOException e) {
                throw new SQLException(e.getMessage(), e);
            } finally {
                try {
                    this.advancedClient.close();
                } catch (IOException e) {
                    throw new SQLException(e.getMessage(), e);
                } finally {
                    this.closed = true;
                }
            }
        }
    }

    // 此方法完全是为了性能测试而存在
    public final ExecuteEnvironment getExecuteEnvironment() {
        return this.executeEnvironment;
    }

    final org.codefamily.crabs.jdbc.lang.Statement analyzeStatement(final String SQL) throws SQLException {
        org.codefamily.crabs.jdbc.lang.Statement statement = this.statementCache.getStatement(SQL);
        if (statement == null) {
            statement = GrammarAnalyzer.analyze(SQL);
            this.statementCache.putStatement(SQL, statement);
        }
        return statement;
    }

    public static final class DatabaseTypeSystem {

        DatabaseTypeSystem(final Connection connection) {
            // to do nothing.
        }

        private HashMap<String, Class<?>> typeMap;

        public final HashMap<String, Class<?>> getTypeMap() throws SQLException {
            HashMap<String, Class<?>> typeMap = this.typeMap;
            if (typeMap == null) {
                typeMap = this.typeMap = new HashMap<String, Class<?>>();
                for (DataType dataType : DataType.values()) {
                    typeMap.put(getTypeName(dataType), getTypeClass(dataType));
                }
            }
            return typeMap;
        }

        public final int getTypeCode(final DataType dataType) throws SQLException {
            switch (dataType) {
                case STRING:
                    return Types.VARCHAR;
                case LONG:
                    return Types.BIGINT;
                case INTEGER:
                    return Types.INTEGER;
                case FLOAT:
                    return Types.FLOAT;
                case DOUBLE:
                    return Types.DOUBLE;
                case BOOLEAN:
                    return Types.BOOLEAN;
                case DATE:
                    return Types.TIMESTAMP;
                default:
                    throw new SQLException("Unsupported data type[" + dataType.name() + "]");
            }
        }

        public final String getTypeName(final DataType dataType)
                throws SQLException {
            return dataType.name();
        }

        public final Class<?> getTypeClass(final DataType dataType) throws SQLException {
            return dataType.getJavaType();
        }

        public final DataType getType(final String typeIdentifier) throws SQLException {
            try {
                return Enum.valueOf(DataType.class, typeIdentifier.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new SQLException("Unsupported data type[" + typeIdentifier + "]");
            }
        }

        public final boolean isTypeSearchable(final DataType dataType) throws SQLException {
            return Comparable.class.isAssignableFrom(dataType.getJavaType());
        }

        public final boolean isTypeCaseSensitive(final DataType dataType) throws SQLException {
            switch (dataType) {
                case STRING:
                    return true;
                case LONG:
                    return false;
                case INTEGER:
                    return false;
                case FLOAT:
                    return false;
                case DOUBLE:
                    return false;
                case BOOLEAN:
                    return false;
                case DATE:
                    return false;
                default:
                    throw new SQLException("Unsupported data type[" + dataType.name() + "]");
            }
        }

    }

    static final class DatabaseMetaData implements java.sql.DatabaseMetaData {

        DatabaseMetaData(final Connection connection) {
            this.connection = connection;
        }

        private final Connection connection;

        @Override
        public final Connection getConnection() throws SQLException {
            return this.connection;
        }

        @Override
        public final String getDatabaseProductName() throws SQLException {
            return Product.NAME;
        }

        @Override
        public final String getDatabaseProductVersion() throws SQLException {
            return Product.VERSION;
        }

        @Override
        public final int getDatabaseMajorVersion() throws SQLException {
            return Product.MAJOR_VERSION;
        }

        @Override
        public final int getDatabaseMinorVersion() throws SQLException {
            return Product.MINOR_VERSION;
        }

        @Override
        public final String getDriverName() throws SQLException {
            return Driver.NAME;
        }

        @Override
        public final String getDriverVersion() throws SQLException {
            return Driver.VERSION;
        }

        @Override
        public final int getDriverMajorVersion() {
            return Driver.MAJOR_VERSION;
        }

        @Override
        public final int getDriverMinorVersion() {
            return Driver.MINOR_VERSION;
        }

        @Override
        public final int getJDBCMajorVersion() throws SQLException {
            return Driver.MAJOR_VERSION;
        }

        @Override
        public final int getJDBCMinorVersion() throws SQLException {
            return Driver.MINOR_VERSION;
        }

        @Override
        public final String getURL() throws SQLException {
            return this.connection.getURL();
        }

        @Override
        public final String getUserName() throws SQLException {
            return "";
        }

        @Override
        public final String getIdentifierQuoteString() throws SQLException {
            return "'";
        }

        @Override
        public final String getSQLKeywords() throws SQLException {
            return "";
        }

        @Override
        public final String getNumericFunctions() throws SQLException {
            return "";
        }

        @Override
        public final String getStringFunctions() throws SQLException {
            return "";
        }

        @Override
        public final String getSystemFunctions() throws SQLException {
            return "";
        }

        @Override
        public final String getTimeDateFunctions() throws SQLException {
            return "";
        }

        @Override
        public final String getSearchStringEscape() throws SQLException {
            return "\\";
        }

        @Override
        public final String getExtraNameCharacters() throws SQLException {
            return "";
        }

        @Override
        public final String getSchemaTerm() throws SQLException {
            return "schema";
        }

        @Override
        public final String getProcedureTerm() throws SQLException {
            return "procedure";
        }

        @Override
        public final String getCatalogTerm() throws SQLException {
            return "catalog";
        }

        @Override
        public final String getCatalogSeparator() throws SQLException {
            return ".";
        }

        @Override
        public final int getMaxBinaryLiteralLength() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxCharLiteralLength() throws SQLException {
            return 4000;
        }

        @Override
        public final int getMaxColumnNameLength() throws SQLException {
            return 200;
        }

        @Override
        public final int getMaxColumnsInGroupBy() throws SQLException {
            return 1;
        }

        @Override
        public final int getMaxColumnsInIndex() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxColumnsInOrderBy() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxColumnsInSelect() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxColumnsInTable() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxConnections() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxCursorNameLength() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxIndexLength() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxSchemaNameLength() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxProcedureNameLength() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxCatalogNameLength() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxRowSize() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxStatementLength() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxStatements() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxTableNameLength() throws SQLException {
            return 0;
        }

        @Override
        public final int getMaxTablesInSelect() throws SQLException {
            return 1;
        }

        @Override
        public final int getMaxUserNameLength() throws SQLException {
            return 0;
        }

        @Override
        public final int getDefaultTransactionIsolation() throws SQLException {
            return this.connection.getTransactionIsolation();
        }

        @Override
        public final ResultSet getSchemas() throws SQLException {
            final int columnCount = 2;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TABLE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("TABLE_SCHEM", DataType.STRING);
            final ArrayList<Object[]> rows = new ArrayList<Object[]>(0);
            rows.add(
                    new Object[]{
                            null,
                            null
                    }
            );
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, rows)
            );
        }

        @Override
        public final ResultSet getSchemas(final String catalog,
                                          final String schemaPattern) throws SQLException {
            return getSchemas();
        }

        @Override
        public final ResultSet getCatalogs() throws SQLException {
            final int columnCount = 1;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TABLE_CAT", DataType.STRING);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getTableTypes() throws SQLException {
            final int columnCount = 1;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TABLE_TYPE", DataType.STRING);
            final ArrayList<Object[]> rows = new ArrayList<Object[]>();
            rows.add(new Object[]{"TABLE"});
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, rows)
            );
        }

        @Override
        public final ResultSet getTables(final String catalog,
                                         final String schemaPattern,
                                         String tableNamePattern,
                                         final String[] types) throws SQLException {
            final int columnCount = 10;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TABLE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("TABLE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("TABLE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("TABLE_TYPE", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("REMARKS", DataType.STRING);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("TYPE_CAT", DataType.STRING);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("TYPE_SCHEM", DataType.STRING);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("TYPE_NAME", DataType.STRING);
            columnInformations[8] = new MetaDataResultSet.ColumnInformation("SELF_REFERENCING_COL_NAME", DataType.STRING);
            columnInformations[9] = new MetaDataResultSet.ColumnInformation("REF_GENERATION", DataType.STRING);
            final ArrayList<Object[]> rows = new ArrayList<Object[]>();
            if (catalog == null || catalog.isEmpty()) {
                tryGetTables:
                {
                    if (types != null) {
                        for (String type : types) {
                            if (type != null
                                    && type.trim().equalsIgnoreCase("TABLE")) {
                                break tryGetTables;
                            }
                        }
                        return new ResultSet(
                                this.connection,
                                null,
                                new MetaDataResultSet(columnInformations, rows)
                        );
                    }
                }
                // get table information.
                final AdvancedClient client = this.connection.advancedClient;
                try {
                    final ReadonlyList<TypeDefinition> typeDefinitionList = client.getTypeDefinitions(
                            this.connection.connectedDatabaseIdentifier
                    );
                    if (tableNamePattern != null) {
                        tableNamePattern
                                = RegularExpressionHelper.toJavaRegularExpression(tableNamePattern);
                    }
                    for (TypeDefinition typeDefinition : typeDefinitionList) {
                        final String tableName = typeDefinition.getIdentifier().toString();
                        if (tableNamePattern != null
                                && !tableName.matches(tableNamePattern)) {
                            continue;
                        }
                        final Object[] rowValues = new Object[columnCount];
                        rowValues[0] = "";
                        rowValues[1] = this.connection.connectedDatabaseIdentifier.toString();
                        rowValues[2] = tableName;
                        rowValues[3] = "TABLE";
                        rowValues[4] = null;
                        rowValues[5] = null;
                        rowValues[6] = null;
                        rowValues[7] = "";
                        rowValues[8] = "";
                        rowValues[9] = null;
                        rows.add(rowValues);
                    }
                } catch (CrabsException ex) {
                    throw new SQLException(ex);
                }
            }
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, rows)
            );
        }

        @Override
        public final ResultSet getColumns(final String catalog,
                                          final String schemaPattern,
                                          String tableNamePattern,
                                          String columnNamePattern) throws SQLException {
            final int columnCount = 23;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TABLE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("TABLE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("TABLE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("COLUMN_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("DATA_TYPE", DataType.INTEGER);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("TYPE_NAME", DataType.STRING);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("COLUMN_SIZE", DataType.INTEGER);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("BUFFER_LENGTH", DataType.INTEGER);
            columnInformations[8] = new MetaDataResultSet.ColumnInformation("DECIMAL_DIGITS", DataType.INTEGER);
            columnInformations[9] = new MetaDataResultSet.ColumnInformation("NUM_PREC_RADIX", DataType.INTEGER);
            columnInformations[10] = new MetaDataResultSet.ColumnInformation("NULLABLE", DataType.INTEGER);
            columnInformations[11] = new MetaDataResultSet.ColumnInformation("REMARKS", DataType.STRING);
            columnInformations[12] = new MetaDataResultSet.ColumnInformation("COLUMN_DEF", DataType.STRING);
            columnInformations[13] = new MetaDataResultSet.ColumnInformation("SQL_DATA_TYPE", DataType.INTEGER);
            columnInformations[14] = new MetaDataResultSet.ColumnInformation("SQL_DATETIME_SUB", DataType.INTEGER);
            columnInformations[15] = new MetaDataResultSet.ColumnInformation("CHAR_OCTET_LENGTH", DataType.INTEGER);
            columnInformations[16] = new MetaDataResultSet.ColumnInformation("ORDINAL_POSITION", DataType.INTEGER);
            columnInformations[17] = new MetaDataResultSet.ColumnInformation("IS_NULLABLE", DataType.STRING);
            columnInformations[18] = new MetaDataResultSet.ColumnInformation("SCOPE_CATLOG", DataType.STRING);
            columnInformations[19] = new MetaDataResultSet.ColumnInformation("SCOPE_SCHEMA", DataType.STRING);
            columnInformations[20] = new MetaDataResultSet.ColumnInformation("SCOPE_TABLE", DataType.STRING);
            columnInformations[21] = new MetaDataResultSet.ColumnInformation("SOURCE_DATA_TYPE", DataType.INTEGER);
            columnInformations[22] = new MetaDataResultSet.ColumnInformation("IS_AUTOINCREMENT", DataType.STRING);
            final ArrayList<Object[]> rows = new ArrayList<Object[]>();
            if (catalog == null || catalog.isEmpty()) {
                // get column information.
                final AdvancedClient client = this.connection.advancedClient;

                try {
                    final ReadonlyList<TypeDefinition> typeDefinitionList = client.getTypeDefinitions(
                            this.connection.connectedDatabaseIdentifier
                    );
                    if (tableNamePattern != null) {
                        tableNamePattern
                                = RegularExpressionHelper.toJavaRegularExpression(tableNamePattern);
                    }
                    if (columnNamePattern != null) {
                        columnNamePattern
                                = RegularExpressionHelper.toJavaRegularExpression(columnNamePattern);
                    }
                    final DatabaseTypeSystem typeSystem = this.connection.getTypeSystem();
                    for (TypeDefinition typeDefinition : typeDefinitionList) {
                        final String tableName = typeDefinition
                                .getIdentifier().toString();
                        if (tableNamePattern != null
                                && !tableName.matches(tableNamePattern)) {
                            continue;
                        }
                        final ReadonlyList<FieldDefinition> columnDefinitionList
                                = typeDefinition.getAllFieldDefinitions();
                        for (int i = 0, columnDefinitionCount = columnDefinitionList
                                .size(); i < columnDefinitionCount; i++) {
                            final FieldDefinition fieldDefinition = columnDefinitionList.get(i);
                            final String columnName = fieldDefinition.getIdentifier().toString();
                            final DataType columnDataType = fieldDefinition.getDataType();
                            if (columnNamePattern != null
                                    && !columnName.matches(columnNamePattern)) {
                                continue;
                            }
                            final Object[] rowValues = new Object[columnCount];
                            rowValues[0] = "";
                            rowValues[1] = "";
                            rowValues[2] = tableName;
                            rowValues[3] = columnName;
                            rowValues[4] = typeSystem.getTypeCode(columnDataType);
                            rowValues[5] = typeSystem.getTypeName(columnDataType);
                            rowValues[6] = columnDataType.getValueSize();
                            rowValues[7] = null;
                            rowValues[8] = null;
                            rowValues[9] = 10;
                            rowValues[10] = columnNullableUnknown;
                            rowValues[11] = null;
                            rowValues[12] = null;
                            rowValues[13] = null;
                            rowValues[14] = null;
                            rowValues[15] = null;
                            rowValues[16] = i + 1;
                            rowValues[17] = "";
                            rowValues[18] = "";
                            rowValues[19] = "";
                            rowValues[20] = tableName;
                            rowValues[21] = (short) (typeSystem.getTypeCode(columnDataType));
                            rowValues[22] = "";
                            rows.add(rowValues);
                        }
                    }
                } catch (CrabsException ex) {
                    throw new SQLException(ex);
                }
            }
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, rows)
            );
        }

        @Override
        public final ResultSet getVersionColumns(final String catalog,
                                                 final String schema,
                                                 final String table) throws SQLException {
            final int columnCount = 8;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("SCOPE", DataType.INTEGER);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("COLUMN_NAME", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("DATA_TYPE", DataType.INTEGER);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("TYPE_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("COLUMN_SIZE", DataType.INTEGER);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("BUFFER_LENGTH", DataType.INTEGER);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("DECIMAL_DIGITS", DataType.INTEGER);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("PSEUDO_COLUMN", DataType.INTEGER);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getTablePrivileges(final String catalog,
                                                  final String schemaPattern,
                                                  final String tableNamePattern)
                throws SQLException {
            final int columnCount = 7;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TABLE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("TABLE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("TABLE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("GRANTOR", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("GRANTEE", DataType.STRING);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("PRIVILEGE", DataType.STRING);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("IS_GRANTABLE", DataType.STRING);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getColumnPrivileges(final String catalog,
                                                   final String schema,
                                                   final String table,
                                                   final String columnNamePattern) throws SQLException {
            final int columnCount = 8;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TABLE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("TABLE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("TABLE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("COLUMN_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("GRANTOR", DataType.STRING);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("GRANTEE", DataType.STRING);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("PRIVILEGE", DataType.STRING);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("IS_GRANTABLE", DataType.STRING);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getBestRowIdentifier(final String catalog,
                                                    final String schema,
                                                    final String table,
                                                    final int scope,
                                                    final boolean nullable) throws SQLException {
            final int columnCount = 8;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("SCOPE", DataType.INTEGER);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("COLUMN_NAME", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("DATA_TYPE", DataType.INTEGER);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("TYPE_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("COLUMN_SIZE", DataType.INTEGER);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("BUFFER_LENGTH", DataType.INTEGER);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("DECIMAL_DIGITS", DataType.INTEGER);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("PSEUDO_COLUMN", DataType.INTEGER);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getPrimaryKeys(final String catalog,
                                              final String schema,
                                              final String table) throws SQLException {
            final int columnCount = 6;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TABLE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("TABLE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("TABLE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("COLUMN_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("KEY_SEQ", DataType.INTEGER);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("PK_NAME", DataType.STRING);
            final ArrayList<Object[]> rows = new ArrayList<Object[]>(1);
            if ((catalog == null || catalog.isEmpty())
                    && (schema == null || schema.isEmpty()) && table != null) {
                // get main keys information.
                final AdvancedClient client = this.connection.advancedClient;
                try {
                    final TypeDefinition typeDefinition = client.getTypeDefinition(
                            this.connection.connectedDatabaseIdentifier,
                            new Identifier(table)
                    );
                    final String tableName = typeDefinition.getIdentifier().toString();
                    final FieldDefinition primaryFieldDefinition = typeDefinition.getPrimaryFieldDefinition();
                    final String primaryFieldName = primaryFieldDefinition.getIdentifier().toString();
                    final Object[] rowValues = new Object[columnCount];
                    rowValues[0] = "";
                    rowValues[1] = "";
                    rowValues[2] = tableName;
                    rowValues[3] = primaryFieldName;
                    rowValues[4] = (short) 1;
                    rowValues[5] = "PK_" + tableName + "_" + primaryFieldName;
                    rows.add(rowValues);
                } catch (CrabsException ex) {
                    throw new SQLException(ex);
                }
            }
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, rows)
            );
        }

        @Override
        public final ResultSet getImportedKeys(final String catalog,
                                               final String schema, final String table) throws SQLException {
            final int columnCount = 14;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("PKTABLE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("PKTABLE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("PKTABLE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("PKCOLUMN_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("FKTABLE_CAT", DataType.STRING);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("FKTABLE_SCHEM", DataType.STRING);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("FKTABLE_NAME", DataType.STRING);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("FKCOLUMN_NAME", DataType.STRING);
            columnInformations[8] = new MetaDataResultSet.ColumnInformation("KEY_SEQ", DataType.INTEGER);
            columnInformations[9] = new MetaDataResultSet.ColumnInformation("UPDATE_RULE", DataType.INTEGER);
            columnInformations[10] = new MetaDataResultSet.ColumnInformation("DELETE_RULE", DataType.INTEGER);
            columnInformations[11] = new MetaDataResultSet.ColumnInformation("FK_NAME", DataType.STRING);
            columnInformations[12] = new MetaDataResultSet.ColumnInformation("PK_NAME", DataType.STRING);
            columnInformations[13] = new MetaDataResultSet.ColumnInformation("DEFERRABILITY", DataType.INTEGER);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getExportedKeys(final String catalog,
                                               final String schema,
                                               final String table) throws SQLException {
            final int columnCount = 14;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("PKTABLE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("PKTABLE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("PKTABLE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("PKCOLUMN_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("FKTABLE_CAT", DataType.STRING);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("FKTABLE_SCHEM", DataType.STRING);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("FKTABLE_NAME", DataType.STRING);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("FKCOLUMN_NAME", DataType.STRING);
            columnInformations[8] = new MetaDataResultSet.ColumnInformation("KEY_SEQ", DataType.INTEGER);
            columnInformations[9] = new MetaDataResultSet.ColumnInformation("UPDATE_RULE", DataType.INTEGER);
            columnInformations[10] = new MetaDataResultSet.ColumnInformation("DELETE_RULE", DataType.INTEGER);
            columnInformations[11] = new MetaDataResultSet.ColumnInformation("FK_NAME", DataType.STRING);
            columnInformations[12] = new MetaDataResultSet.ColumnInformation("PK_NAME", DataType.STRING);
            columnInformations[13] = new MetaDataResultSet.ColumnInformation("DEFERRABILITY", DataType.INTEGER);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getCrossReference(final String parentCatalog,
                                                 final String parentSchema,
                                                 final String parentTable,
                                                 final String foreignCatalog,
                                                 final String foreignSchema,
                                                 final String foreignTable) throws SQLException {
            final int columnCount = 14;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("PKTABLE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("PKTABLE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("PKTABLE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("PKCOLUMN_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("FKTABLE_CAT", DataType.STRING);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("FKTABLE_SCHEM", DataType.STRING);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("FKTABLE_NAME", DataType.STRING);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("FKCOLUMN_NAME", DataType.STRING);
            columnInformations[8] = new MetaDataResultSet.ColumnInformation("KEY_SEQ", DataType.INTEGER);
            columnInformations[9] = new MetaDataResultSet.ColumnInformation("UPDATE_RULE", DataType.INTEGER);
            columnInformations[10] = new MetaDataResultSet.ColumnInformation("DELETE_RULE", DataType.INTEGER);
            columnInformations[11] = new MetaDataResultSet.ColumnInformation("FK_NAME", DataType.STRING);
            columnInformations[12] = new MetaDataResultSet.ColumnInformation("PK_NAME", DataType.STRING);
            columnInformations[13] = new MetaDataResultSet.ColumnInformation("DEFERRABILITY", DataType.INTEGER);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getTypeInfo() throws SQLException {
            final int columnCount = 18;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TYPE_NAME", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("DATA_TYPE", DataType.INTEGER);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("PRECISION", DataType.INTEGER);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("LITERAL_PREFIX", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("LITERAL_SUFFIX", DataType.STRING);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("CREATE_PARAMS", DataType.STRING);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("NULLABLE", DataType.INTEGER);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("CASE_SENSITIVE", DataType.BOOLEAN);
            columnInformations[8] = new MetaDataResultSet.ColumnInformation("SEARCHABLE", DataType.INTEGER);
            columnInformations[9] = new MetaDataResultSet.ColumnInformation("UNSIGNED_ATTRIBUTE", DataType.BOOLEAN);
            columnInformations[10] = new MetaDataResultSet.ColumnInformation("FIXED_PREC_SCALE", DataType.BOOLEAN);
            columnInformations[11] = new MetaDataResultSet.ColumnInformation("AUTO_INCREMENT", DataType.BOOLEAN);
            columnInformations[12] = new MetaDataResultSet.ColumnInformation("LOCAL_TYPE_NAME", DataType.STRING);
            columnInformations[13] = new MetaDataResultSet.ColumnInformation("MINIMUM_SCALE", DataType.INTEGER);
            columnInformations[14] = new MetaDataResultSet.ColumnInformation("MAXIMUM_SCALE", DataType.INTEGER);
            columnInformations[15] = new MetaDataResultSet.ColumnInformation("SQL_DATA_TYPE", DataType.INTEGER);
            columnInformations[16] = new MetaDataResultSet.ColumnInformation("SQL_DATETIME_SUB", DataType.INTEGER);
            columnInformations[17] = new MetaDataResultSet.ColumnInformation("NUM_PREC_RADIX", DataType.INTEGER);
            final ArrayList<Object[]> rows = new ArrayList<Object[]>();
            final DatabaseTypeSystem typeSystem = this.connection.getTypeSystem();
            for (DataType dataType : DataType.values()) {
                final Object[] rowValues = new Object[columnCount];
                rowValues[0] = typeSystem.getTypeName(dataType);
                rowValues[1] = typeSystem.getTypeCode(dataType);
                rowValues[2] = 0;
                rowValues[3] = null;
                rowValues[4] = null;
                rowValues[5] = null;
                rowValues[6] = (short) typeNullableUnknown;
                rowValues[7] = typeSystem.isTypeCaseSensitive(dataType);
                rowValues[8] = (short) (typeSystem.isTypeSearchable(dataType) ? typeSearchable : typePredNone);
                rowValues[9] = false;
                rowValues[10] = false;
                rowValues[11] = false;
                rowValues[12] = null;
                rowValues[13] = (short) 0;
                rowValues[14] = Short.MAX_VALUE;
                rowValues[15] = null;
                rowValues[16] = null;
                rowValues[17] = 10;
                rows.add(rowValues);
            }
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, rows)
            );
        }

        @Override
        public final ResultSet getIndexInfo(final String catalog,
                                            final String schema,
                                            final String table,
                                            final boolean unique,
                                            final boolean approximate) throws SQLException {
            final int columnCount = 13;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TABLE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("TABLE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("TABLE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("NON_UNIQUE", DataType.BOOLEAN);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("INDEX_QUALIFIER", DataType.STRING);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("INDEX_NAME", DataType.STRING);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("TYPE", DataType.INTEGER);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("ORDINAL_POSITION", DataType.INTEGER);
            columnInformations[8] = new MetaDataResultSet.ColumnInformation("COLUMN_NAME", DataType.STRING);
            columnInformations[9] = new MetaDataResultSet.ColumnInformation("ASC_OR_DESC", DataType.STRING);
            columnInformations[10] = new MetaDataResultSet.ColumnInformation("CARDINALITY", DataType.INTEGER);
            columnInformations[11] = new MetaDataResultSet.ColumnInformation("PAGES", DataType.INTEGER);
            columnInformations[12] = new MetaDataResultSet.ColumnInformation("FILTER_CONDITION", DataType.STRING);
            final ArrayList<Object[]> rows = new ArrayList<Object[]>(1);
            if ((catalog == null || catalog.isEmpty())
                    && (schema == null || schema.isEmpty()) && table != null) {
                // get index information.
                final AdvancedClient client = this.connection.advancedClient;
                try {
                    final TypeDefinition typeDefinition = client.getTypeDefinition(
                            this.connection.connectedDatabaseIdentifier,
                            new Identifier(table)
                    );
                    final String tableName = typeDefinition.getIdentifier().toString();
                    final FieldDefinition primaryFieldDefinition = typeDefinition.getPrimaryFieldDefinition();
                    final String primaryFieldName = primaryFieldDefinition.getIdentifier().toString();
                    final Object[] rowValues = new Object[columnCount];
                    rowValues[0] = "";
                    rowValues[1] = "";
                    rowValues[2] = tableName;
                    rowValues[3] = true;
                    rowValues[4] = null;
                    rowValues[5] = "PK_" + tableName + "_" + primaryFieldName;
                    rowValues[6] = tableIndexOther;
                    rowValues[7] = (short) 1;
                    rowValues[8] = primaryFieldName;
                    rowValues[9] = null;
                    rowValues[10] = null;
                    rowValues[11] = null;
                    rowValues[12] = null;
                    rows.add(rowValues);
                } catch (CrabsException ex) {
                    throw new SQLException(ex);
                }
            }
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, rows)
            );
        }

        @Override
        public final ResultSet getUDTs(final String catalog,
                                       final String schemaPattern,
                                       final String typeNamePattern,
                                       final int[] types) throws SQLException {
            final int columnCount = 7;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TYPE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("TYPE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("TYPE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("CLASS_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("DATA_TYPE", DataType.INTEGER);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("REMARKS", DataType.STRING);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("BASE_TYPE", DataType.INTEGER);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getSuperTypes(final String catalog,
                                             final String schemaPattern,
                                             final String typeNamePattern) throws SQLException {
            final int columnCount = 6;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TYPE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("TYPE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("TYPE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("SUPERTYPE_CAT", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("SUPERTYPE_SCHEM", DataType.STRING);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("SUPERTYPE_NAME", DataType.STRING);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getSuperTables(final String catalog,
                                              final String schemaPattern,
                                              final String tableNamePattern) throws SQLException {
            final int columnCount = 4;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TABLE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("TABLE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("TABLE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("SUPERTABLE_NAME", DataType.STRING);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getAttributes(final String catalog,
                                             final String schemaPattern,
                                             final String typeNamePattern,
                                             final String attributeNamePattern) throws SQLException {
            final int columnCount = 21;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TYPE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("TYPE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("TYPE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("ATTR_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("DATA_TYPE", DataType.INTEGER);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("ATTR_TYPE_NAME", DataType.STRING);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("ATTR_SIZE", DataType.INTEGER);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("DECIMAL_DIGITS", DataType.INTEGER);
            columnInformations[8] = new MetaDataResultSet.ColumnInformation("NUM_PREC_RADIX", DataType.INTEGER);
            columnInformations[9] = new MetaDataResultSet.ColumnInformation("NULLABLE", DataType.INTEGER);
            columnInformations[10] = new MetaDataResultSet.ColumnInformation("REMARKS", DataType.STRING);
            columnInformations[11] = new MetaDataResultSet.ColumnInformation("ATTR_DEF", DataType.STRING);
            columnInformations[12] = new MetaDataResultSet.ColumnInformation("SQL_DATA_TYPE", DataType.INTEGER);
            columnInformations[13] = new MetaDataResultSet.ColumnInformation("SQL_DATETIME_SUB", DataType.INTEGER);
            columnInformations[14] = new MetaDataResultSet.ColumnInformation("CHAR_OCTET_LENGTH", DataType.INTEGER);
            columnInformations[15] = new MetaDataResultSet.ColumnInformation("ORDINAL_POSITION", DataType.INTEGER);
            columnInformations[16] = new MetaDataResultSet.ColumnInformation("IS_NULLABLE", DataType.STRING);
            columnInformations[17] = new MetaDataResultSet.ColumnInformation("SCOPE_CATALOG", DataType.STRING);
            columnInformations[18] = new MetaDataResultSet.ColumnInformation("SCOPE_SCHEMA", DataType.STRING);
            columnInformations[19] = new MetaDataResultSet.ColumnInformation("SCOPE_TABLE", DataType.STRING);
            columnInformations[20] = new MetaDataResultSet.ColumnInformation("SOURCE_DATA_TYPE", DataType.INTEGER);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getClientInfoProperties() throws SQLException {
            final int columnCount = 4;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("NAME", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("MAX_LEN", DataType.INTEGER);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("DEFAULT_VALUE", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("DESCRIPTION", DataType.STRING);
            final ArrayList<Object[]> rows = new ArrayList<Object[]>();
            for (PropertyEntry propertyEntry : Protocol.getPropertyEntryList()) {
                final Object[] rowValues = new Object[columnCount];
                rowValues[0] = propertyEntry.identifier;
                rowValues[1] = propertyEntry.maxinumValueLength;
                rowValues[2] = propertyEntry.defaultValue;
                rowValues[3] = propertyEntry.description;
                rows.add(rowValues);
            }
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, rows)
            );
        }

        @Override
        public final ResultSet getProcedures(final String catalog,
                                             final String schemaPattern,
                                             final String procedureNamePattern) throws SQLException {
            final int columnCount = 6;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("PROCEDURE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("PROCEDURE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("PROCEDURE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("REMARKS", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("PROCEDURE_TYPE", DataType.INTEGER);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("SPECIFIC_NAME", DataType.STRING);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getProcedureColumns(final String catalog,
                                                   final String schemaPattern,
                                                   final String procedureNamePattern,
                                                   final String columnNamePattern) throws SQLException {
            final int columnCount = 20;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("PROCEDURE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("PROCEDURE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("PROCEDURE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("COLUMN_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("COLUMN_TYPE", DataType.INTEGER);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("DATA_TYPE", DataType.INTEGER);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("TYPE_NAME", DataType.STRING);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("PRECISION", DataType.INTEGER);
            columnInformations[8] = new MetaDataResultSet.ColumnInformation("LENGTH", DataType.INTEGER);
            columnInformations[9] = new MetaDataResultSet.ColumnInformation("SCALE", DataType.INTEGER);
            columnInformations[10] = new MetaDataResultSet.ColumnInformation("RADIX", DataType.INTEGER);
            columnInformations[11] = new MetaDataResultSet.ColumnInformation("NULLABLE", DataType.INTEGER);
            columnInformations[12] = new MetaDataResultSet.ColumnInformation("REMARKS", DataType.STRING);
            columnInformations[13] = new MetaDataResultSet.ColumnInformation("COLUMN_DEF", DataType.STRING);
            columnInformations[14] = new MetaDataResultSet.ColumnInformation("SQL_DATA_TYPE", DataType.INTEGER);
            columnInformations[15] = new MetaDataResultSet.ColumnInformation("SQL_DATETIME_SUB", DataType.INTEGER);
            columnInformations[16] = new MetaDataResultSet.ColumnInformation("CHAR_OCTET_LENGTH", DataType.INTEGER);
            columnInformations[17] = new MetaDataResultSet.ColumnInformation("ORDINAL_POSITION", DataType.INTEGER);
            columnInformations[18] = new MetaDataResultSet.ColumnInformation("IS_NULLABLE", DataType.STRING);
            columnInformations[19] = new MetaDataResultSet.ColumnInformation("SPECIFIC_NAME", DataType.STRING);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getFunctions(final String catalog,
                                            final String schemaPattern,
                                            final String functionNamePattern) throws SQLException {
            final int columnCount = 6;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("FUNCTION_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("FUNCTION_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("FUNCTION_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("REMARKS", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("FUNCTION_TYPE", DataType.INTEGER);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("SPECIFIC_NAME", DataType.STRING);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getFunctionColumns(final String catalog,
                                                  final String schemaPattern,
                                                  final String functionNamePattern,
                                                  final String columnNamePattern) throws SQLException {
            final int columnCount = 17;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("FUNCTION_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("FUNCTION_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("FUNCTION_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("COLUMN_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("COLUMN_TYPE", DataType.INTEGER);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("DATA_TYPE", DataType.INTEGER);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("TYPE_NAME", DataType.STRING);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("PRECISION", DataType.INTEGER);
            columnInformations[8] = new MetaDataResultSet.ColumnInformation("LENGTH", DataType.INTEGER);
            columnInformations[9] = new MetaDataResultSet.ColumnInformation("SCALE", DataType.INTEGER);
            columnInformations[10] = new MetaDataResultSet.ColumnInformation("RADIX", DataType.INTEGER);
            columnInformations[11] = new MetaDataResultSet.ColumnInformation("NULLABLE", DataType.INTEGER);
            columnInformations[12] = new MetaDataResultSet.ColumnInformation("REMARKS", DataType.STRING);
            columnInformations[13] = new MetaDataResultSet.ColumnInformation("CHAR_OCTET_LENGTH", DataType.INTEGER);
            columnInformations[14] = new MetaDataResultSet.ColumnInformation("ORDINAL_POSITION", DataType.INTEGER);
            columnInformations[15] = new MetaDataResultSet.ColumnInformation("IS_NULLABLE", DataType.STRING);
            columnInformations[16] = new MetaDataResultSet.ColumnInformation("SPECIFIC_NAME", DataType.STRING);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final ResultSet getPseudoColumns(final String catalog,
                                                final String schemaPattern,
                                                final String tableNamePattern,
                                                final String columnNamePattern) throws SQLException {
            final int columnCount = 12;
            final MetaDataResultSet.ColumnInformation[] columnInformations
                    = new MetaDataResultSet.ColumnInformation[columnCount];
            columnInformations[0] = new MetaDataResultSet.ColumnInformation("TABLE_CAT", DataType.STRING);
            columnInformations[1] = new MetaDataResultSet.ColumnInformation("TABLE_SCHEM", DataType.STRING);
            columnInformations[2] = new MetaDataResultSet.ColumnInformation("TABLE_NAME", DataType.STRING);
            columnInformations[3] = new MetaDataResultSet.ColumnInformation("COLUMN_NAME", DataType.STRING);
            columnInformations[4] = new MetaDataResultSet.ColumnInformation("DATA_TYPE", DataType.INTEGER);
            columnInformations[5] = new MetaDataResultSet.ColumnInformation("COLUMN_SIZE", DataType.INTEGER);
            columnInformations[6] = new MetaDataResultSet.ColumnInformation("DECIMAL_DIGITS", DataType.INTEGER);
            columnInformations[7] = new MetaDataResultSet.ColumnInformation("NUM_PREC_RADIX", DataType.INTEGER);
            columnInformations[8] = new MetaDataResultSet.ColumnInformation("COLUMN_USAGE", DataType.STRING);
            columnInformations[9] = new MetaDataResultSet.ColumnInformation("REMARKS", DataType.STRING);
            columnInformations[10] = new MetaDataResultSet.ColumnInformation("CHAR_OCTET_LENGTH", DataType.INTEGER);
            columnInformations[11] = new MetaDataResultSet.ColumnInformation("IS_NULLABLE", DataType.STRING);
            return new ResultSet(
                    this.connection,
                    null,
                    new MetaDataResultSet(columnInformations, new ArrayList<Object[]>(0))
            );
        }

        @Override
        public final boolean generatedKeyAlwaysReturned() throws SQLException {
            return true;
        }

        @Override
        public final int getResultSetHoldability() throws SQLException {
            return this.connection.getHoldability();
        }

        @Override
        public final int getSQLStateType() throws SQLException {
            return java.sql.DatabaseMetaData.sqlStateSQL99;
        }

        @Override
        public final RowIdLifetime getRowIdLifetime() throws SQLException {
            return RowIdLifetime.ROWID_UNSUPPORTED;
        }

        @Override
        public final boolean supportsAlterTableWithAddColumn()
                throws SQLException {
            return true;
        }

        @Override
        public final boolean supportsAlterTableWithDropColumn()
                throws SQLException {
            return true;
        }

        @Override
        public final boolean supportsColumnAliasing() throws SQLException {
            return true;
        }

        @Override
        public final boolean supportsMixedCaseIdentifiers() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsMixedCaseQuotedIdentifiers()
                throws SQLException {
            return true;
        }

        @Override
        public final boolean supportsConvert() throws SQLException {
            return true;
        }

        @Override
        public final boolean supportsConvert(final int fromType,
                                             final int toType) throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsTableCorrelationNames()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsDifferentTableCorrelationNames()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsExpressionsInOrderBy() throws SQLException {
            return true;
        }

        @Override
        public final boolean supportsOrderByUnrelated() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsGroupBy() throws SQLException {
            return true;
        }

        @Override
        public final boolean supportsGroupByUnrelated() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsGroupByBeyondSelect() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsLikeEscapeClause() throws SQLException {
            return true;
        }

        @Override
        public final boolean supportsMultipleResultSets() throws SQLException {
            return true;
        }

        @Override
        public final boolean supportsMultipleTransactions() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsNonNullableColumns() throws SQLException {
            return true;
        }

        @Override
        public final boolean supportsMinimumSQLGrammar() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsCoreSQLGrammar() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsExtendedSQLGrammar() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsANSI92EntryLevelSQL() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsANSI92IntermediateSQL()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsANSI92FullSQL() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsIntegrityEnhancementFacility()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsOuterJoins() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsFullOuterJoins() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsLimitedOuterJoins() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsSchemasInDataManipulation()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsSchemasInProcedureCalls()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsSchemasInTableDefinitions() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsSchemasInIndexDefinitions()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsSchemasInPrivilegeDefinitions()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsCatalogsInDataManipulation()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsCatalogsInProcedureCalls()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsCatalogsInTableDefinitions()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsCatalogsInIndexDefinitions()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsCatalogsInPrivilegeDefinitions()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsPositionedDelete() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsPositionedUpdate() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsSelectForUpdate() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsStoredProcedures() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsSubqueriesInComparisons()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsSubqueriesInExists() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsSubqueriesInIns() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsSubqueriesInQuantifieds()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsCorrelatedSubqueries() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsUnion() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsUnionAll() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsOpenCursorsAcrossCommit()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsOpenCursorsAcrossRollback()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsOpenStatementsAcrossCommit()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsOpenStatementsAcrossRollback()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsTransactions() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsTransactionIsolationLevel(final int level) throws SQLException {
            return level == this.connection.getTransactionIsolation();
        }

        @Override
        public final boolean supportsDataDefinitionAndDataManipulationTransactions()
                throws SQLException {
            return true;
        }

        @Override
        public final boolean supportsDataManipulationTransactionsOnly()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsResultSetType(final int type)
                throws SQLException {
            return type == ResultSet.TYPE_FORWARD_ONLY;
        }

        @Override
        public final boolean supportsResultSetConcurrency(final int type,
                                                          final int concurrency) throws SQLException {
            return type == ResultSet.TYPE_FORWARD_ONLY
                    && concurrency == Connection.TRANSACTION_READ_COMMITTED;
        }

        @Override
        public final boolean supportsBatchUpdates() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsSavepoints() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsNamedParameters() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsMultipleOpenResults() throws SQLException {
            return true;
        }

        @Override
        public final boolean supportsGetGeneratedKeys() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsResultSetHoldability(final int holdability)
                throws SQLException {
            return holdability == this.connection.getHoldability();
        }

        @Override
        public final boolean supportsStatementPooling() throws SQLException {
            return false;
        }

        @Override
        public final boolean supportsStoredFunctionsUsingCallSyntax()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean isCatalogAtStart() throws SQLException {
            return false;
        }

        @Override
        public final boolean isWrapperFor(final Class<?> iface)
                throws SQLException {
            return iface.isInstance(this);
        }

        @Override
        public final boolean isReadOnly() throws SQLException {
            return false;
        }

        @Override
        public final boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
            return false;
        }

        @Override
        public final boolean dataDefinitionCausesTransactionCommit()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean dataDefinitionIgnoredInTransactions()
                throws SQLException {
            return false;
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
        public final boolean allProceduresAreCallable() throws SQLException {
            return false;
        }

        @Override
        public final boolean allTablesAreSelectable() throws SQLException {
            return true;
        }

        @Override
        public final boolean nullsAreSortedHigh() throws SQLException {
            return false;
        }

        @Override
        public final boolean nullsAreSortedLow() throws SQLException {
            return true;
        }

        @Override
        public final boolean nullsAreSortedAtStart() throws SQLException {
            return true;
        }

        @Override
        public final boolean nullsAreSortedAtEnd() throws SQLException {
            return false;
        }

        @Override
        public final boolean nullPlusNonNullIsNull() throws SQLException {
            return true;
        }

        @Override
        public final boolean usesLocalFiles() throws SQLException {
            return false;
        }

        @Override
        public final boolean usesLocalFilePerTable() throws SQLException {
            return false;
        }

        @Override
        public final boolean storesUpperCaseIdentifiers() throws SQLException {
            return true;
        }

        @Override
        public final boolean storesLowerCaseIdentifiers() throws SQLException {
            return false;
        }

        @Override
        public final boolean storesMixedCaseIdentifiers() throws SQLException {
            return false;
        }

        @Override
        public final boolean storesUpperCaseQuotedIdentifiers()
                throws SQLException {
            return true;
        }

        @Override
        public final boolean storesLowerCaseQuotedIdentifiers()
                throws SQLException {
            return false;
        }

        @Override
        public final boolean storesMixedCaseQuotedIdentifiers()
                throws SQLException {
            return true;
        }

        @Override
        public final boolean ownUpdatesAreVisible(final int type)
                throws SQLException {
            return false;
        }

        @Override
        public final boolean ownDeletesAreVisible(final int type)
                throws SQLException {
            return false;
        }

        @Override
        public final boolean ownInsertsAreVisible(final int type)
                throws SQLException {
            return false;
        }

        @Override
        public final boolean othersUpdatesAreVisible(final int type)
                throws SQLException {
            return true;
        }

        @Override
        public final boolean othersDeletesAreVisible(final int type)
                throws SQLException {
            return true;
        }

        @Override
        public final boolean othersInsertsAreVisible(final int type)
                throws SQLException {
            return true;
        }

        @Override
        public final boolean updatesAreDetected(final int type)
                throws SQLException {
            return false;
        }

        @Override
        public final boolean deletesAreDetected(final int type)
                throws SQLException {
            return false;
        }

        @Override
        public final boolean insertsAreDetected(final int type)
                throws SQLException {
            return false;
        }

        @Override
        public final boolean locatorsUpdateCopy() throws SQLException {
            return false;
        }

        @Override
        public final boolean autoCommitFailureClosesAllResultSets()
                throws SQLException {
            return false;
        }

        static final class MetaDataResultSet extends InternalResultSet {

            MetaDataResultSet(final MetaDataResultSet.ColumnInformation[] columnInformations,
                              final Collection<Object[]> rows) {
                this.metaData = new MetaDataImplementation(columnInformations);
                this.rowIterator = rows.iterator();
            }

            private final MetaDataImplementation metaData;

            private final Iterator<Object[]> rowIterator;

            private Object[] currentRow;

            @Override
            public final InternalMetaData getMetaData() {
                return this.metaData;
            }

            @Override
            public final Object getColumnValue(final Identifier columnIdentifier) {
                return this.currentRow[this.metaData.getColumnIndex(columnIdentifier)];
            }

            @Override
            public final Object getColumnValue(final int columnIndex) {
                return this.currentRow[columnIndex];
            }

            @Override
            public final boolean next() throws CrabsException {
                if (this.rowIterator.hasNext()) {
                    this.currentRow = this.rowIterator.next();
                    return true;
                } else {
                    this.currentRow = null;
                    return false;
                }
            }

            @Override
            public final void close() throws IOException {
                // to do nothing.
            }

            static final class ColumnInformation {

                ColumnInformation(final String identifier,
                                  final DataType dataType) throws SQLException {
                    this.identifier = new Identifier(identifier);
                    this.dataType = dataType;
                    this.displaySize = dataType.displaySize();
                }

                final Identifier identifier;

                final DataType dataType;

                final int displaySize;

            }

            private static final class MetaDataImplementation extends InternalMetaData {

                MetaDataImplementation(final MetaDataResultSet.ColumnInformation[] columnInformations) {
                    final HashMap<Identifier, Integer> columnIndexMap = new HashMap<Identifier, Integer>();
                    for (int i = 0; i < columnInformations.length; i++) {
                        final ColumnInformation columnInformation = columnInformations[i];
                        columnIndexMap.put(columnInformation.identifier, i);
                    }
                    this.columnInformations = columnInformations;
                    this.columnIndexMap = columnIndexMap;
                }

                private final MetaDataResultSet.ColumnInformation[] columnInformations;

                private final HashMap<Identifier, Integer> columnIndexMap;

                @Override
                public final int getColumnCount() {
                    return this.columnInformations.length;
                }

                @Override
                public final int getColumnIndex(final Identifier columnIdentifier) {
                    if (columnIdentifier == null) {
                        throw new IllegalArgumentException("Argument [columnIdentifier] is null.");
                    }
                    final Integer columnIndex = this.columnIndexMap.get(columnIdentifier);
                    return columnIndex == null ? -1 : columnIndex;
                }

                @Override
                public final Identifier getColumnIdentifier(
                        final int columnIndex) {
                    return this.columnInformations[columnIndex].identifier;
                }

                @Override
                public final String getColumnLabel(final int columnIndex) {
                    return this.getColumnIdentifier(columnIndex).toString();
                }

                @Override
                public final DataType getColumnValueType(final int columnIndex) {
                    return this.columnInformations[columnIndex].dataType;
                }

                @Override
                public final int getColumnDisplaySize(final int columnIndex) {
                    return this.columnInformations[columnIndex].displaySize;
                }

            }

        }

    }

}
