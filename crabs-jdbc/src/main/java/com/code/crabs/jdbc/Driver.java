package com.code.crabs.jdbc;

import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public final class Driver implements java.sql.Driver {

    public static final int MAJOR_VERSION = 1;

    public static final int MINOR_VERSION = 0;

    public static final String NAME = Driver.class.getName();

    public static final String VERSION = MAJOR_VERSION + "." + MINOR_VERSION;

    private static final DriverPropertyInfo[] EMPTY_DRIVER_PROPERTIES = new DriverPropertyInfo[0];

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to register driver[" + NAME + "]: " + e.getMessage());
        }
    }

    public Driver() {
        // to do nothing.
    }

    public final String getName() {
        return NAME;
    }

    @Override
    public final int getMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public final int getMinorVersion() {
        return MINOR_VERSION;
    }

    @Override
    public final DriverPropertyInfo[] getPropertyInfo(final String URL,
                                                      final Properties properties) throws SQLException {
        return EMPTY_DRIVER_PROPERTIES;
    }

    @Override
    public final boolean acceptsURL(final String URL) throws SQLException {
        if (URL == null) {
            throw new IllegalArgumentException("Argument [URL] is null.");
        }
        return URL.startsWith(Protocol.JDBC_PREFIX);
    }

    @Override
    public final Connection connect(final String URL,
                                    Properties properties) throws SQLException {
        if (!acceptsURL(URL)) {
            throw new SQLException("Driver does not understand the given URL, it must be start with '"
                    + Protocol.JDBC_PREFIX + "'");
        }
        if (properties == null) {
            properties = new Properties();
        }
        properties.setProperty(
                Protocol.PROPERTY_ENTRY$DRIVER_NAME.identifier,
                this.getClass().getName()
        );
        properties.setProperty(
                Protocol.PROPERTY_ENTRY$DRIVER_MAJOR_VERSION.identifier,
                Integer.toString(getMajorVersion())
        );
        properties.setProperty(
                Protocol.PROPERTY_ENTRY$DRIVER_MINOR_VERSION.identifier,
                Integer.toString(getMinorVersion())
        );
        return new Connection(URL, properties);
    }

    @Override
    public final boolean jdbcCompliant() {
        return false;
    }

    @Override
    public final Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

}
