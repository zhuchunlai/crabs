package com.code.crabs.jdbc;

import com.code.crabs.core.client.AdvancedClient.ElasticsearchAddress;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public final class DataSource implements javax.sql.DataSource {

    DataSource(final ElasticsearchAddress[] addresses,
               final String database,
               final String username,
               final String password) {
        if (addresses == null || addresses.length == 0) {
            throw new IllegalArgumentException("Argument[addresses] is null.");
        }
        final int length = addresses.length;
        final ElasticsearchAddress[] finallyAddresses = new ElasticsearchAddress[length];
        ElasticsearchAddress address;
        for (int index = 0; index < length; index++) {
            address = addresses[index];
            if (address == null) {
                throw new IllegalArgumentException("Argument[addresses[" + index + "]] is null.");
            }
            finallyAddresses[index] = address;
        }
        this.addresses = finallyAddresses;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    private final ElasticsearchAddress[] addresses;

    private final String database;

    private final String username;

    private final String password;

    private PrintWriter logWriter;

    private int loginTimeout;

    @Override
    public final PrintWriter getLogWriter() throws SQLException {
        return this.logWriter;
    }

    @Override
    public final int getLoginTimeout() throws SQLException {
        return this.loginTimeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public final Connection getConnection() throws SQLException {
        return this.getConnection(this.username, this.password);
    }

    @Override
    public final Connection getConnection(final String username,
                                          final String password) throws SQLException {
        if (username == null) {
            throw new IllegalArgumentException("Argument[username] is null.");
        }
        if (password == null) {
            throw new IllegalArgumentException("Argument[password] is null.");
        }
        final StringBuilder URLBuilder = new StringBuilder();
        URLBuilder.append(Protocol.JDBC_PREFIX);
        URLBuilder.append(':');
        String delimiter = "";
        for (int index = 0, length = this.addresses.length; index < length; index++) {
            URLBuilder.append(delimiter).append(this.addresses[index]);
            delimiter = ",";
        }
        URLBuilder.append("/").append(this.database);
        return new Connection(URLBuilder.toString(), new Properties());
    }

    @Override
    public final boolean isWrapperFor(final Class<?> iface) throws SQLException {
        if (iface == null) {
            throw new IllegalArgumentException("Argument [iface] is null.");
        }
        return iface.isInstance(this);
    }

    @Override
    public final void setLogWriter(final PrintWriter logWriter)
            throws SQLException {
        this.logWriter = logWriter;
    }

    @Override
    public final void setLoginTimeout(final int loginTimeout)
            throws SQLException {
        this.loginTimeout = loginTimeout;
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

}
