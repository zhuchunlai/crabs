package com.code.crabs.jdbc;

import com.code.crabs.Product;
import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.common.util.StringUtils;
import com.code.crabs.core.client.AdvancedClient.ElasticsearchAddress;
import com.code.crabs.exception.SQL4ESException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Properties;

/**
 * JDBC连接协议模型
 * 协议格式：jdbc:elasticsearch://ip:port[{,ip:port}...]/indexName?key=value[{&key=value}...]
 *
 * @author zhuchunlai
 * @version $Id: Protocol.java, v1.0 2014/08/13 14:25 $
 */
public final class Protocol {

    public static final String JDBC_PREFIX = "jdbc:elasticsearch://";

    private static final String JDBC_URL_FORMAT = JDBC_PREFIX
            + "ip:port[{,ip:port}...]/indexName?key=value[{&key=value}...]";

    private static final Properties EMPTY_PROPERTIES = new Properties();

    public static Protocol parseURL(String URL) throws SQL4ESException {
        URL = URL.trim().substring(JDBC_PREFIX.length());
        final int slashIndex = URL.indexOf('/');
        if (slashIndex == -1) {
            throw new SQL4ESException("Invalid URL, expected format is: " + JDBC_URL_FORMAT);
        }
        final ElasticsearchAddress[] addresses;
        try {
            addresses = ElasticsearchAddress.toElasticsearchAddresses(URL.substring(0, slashIndex));
        } catch (Exception e) {
            throw new SQL4ESException("Invalid URL, expected format is: " + JDBC_URL_FORMAT);
        }
        URL = URL.substring(slashIndex + 1);
        final int questionMarkIndex = URL.indexOf('?');
        final String databaseName;
        final Properties properties;
        if (questionMarkIndex == -1) {
            databaseName = URL;
            properties = EMPTY_PROPERTIES;
        } else {
            databaseName = URL.substring(0, questionMarkIndex);
            properties = new Properties();
            URL = URL.substring(questionMarkIndex + 1);
            final String[] keyValueMappings = URL.split("&");
            String keyValueString;
            String[] keyValue;
            String key, value;
            for (int index = 0, size = keyValueMappings.length; index < size; index++) {
                keyValueString = keyValueMappings[index];
                if (StringUtils.isNullOrEmptyAfterTrim(keyValueString)) {
                    throw new SQL4ESException("Invalid URL, expected format is: " + JDBC_URL_FORMAT);
                }
                keyValue = keyValueString.split("=");
                if (keyValue.length != 2) {
                    throw new SQL4ESException("Invalid URL, expected format is: " + JDBC_URL_FORMAT);
                }
                key = keyValue[0];
                value = keyValue[1];
                if (StringUtils.isNullOrEmptyAfterTrim(key)
                        || StringUtils.isNullOrEmptyAfterTrim(value)) {
                    throw new SQL4ESException("Invalid URL, expected format is: " + JDBC_URL_FORMAT);
                }
                properties.put(keyValue[0].trim(), keyValue[1].trim());
            }
        }
        return new Protocol(addresses, databaseName, properties);
    }

    public final ElasticsearchAddress[] getServerAddresses() {
        return this.elasticsearchAddresses.clone();
    }

    public final String getDatabaseName() {
        return this.databaseName;
    }

    public final Properties getProperties() {
        return this.properties;
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj != null && obj instanceof Protocol) {
            final Protocol that = (Protocol) obj;
            if (this.elasticsearchAddresses.length != that.elasticsearchAddresses.length) {
                return false;
            }
            for (int index = 0, length = this.elasticsearchAddresses.length; index < length; index++) {
                if (!this.elasticsearchAddresses[index].equals(that.elasticsearchAddresses[index])) {
                    return false;
                }
            }
            if (!this.databaseName.equals(that.databaseName)) {
                return false;
            }
            if (this.properties.size() != this.properties.size()) {
                return false;
            }
            for (Object key : this.properties.keySet()) {
                if (!this.properties.get(key).equals(that.properties.get(key))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private final String databaseName;

    private final ElasticsearchAddress[] elasticsearchAddresses;

    private final Properties properties;

    Protocol(final ElasticsearchAddress[] addresses,
                     final String databaseName,
                     final Properties properties) {
        this.elasticsearchAddresses = addresses;
        this.databaseName = databaseName;
        this.properties = properties;
    }

    public static final PropertyEntry PROPERTY_ENTRY$SCAN_SIZE
            = new PropertyEntry("clientScanSize", Integer.MAX_VALUE, "500", "Client scan size.");

    public static final PropertyEntry PROPERTY_ENTRY$DRIVER_NAME
            = new PropertyEntry("driverName", Integer.MAX_VALUE, Driver.class.getName(), "Driver name.");

    public static final PropertyEntry PROPERTY_ENTRY$DRIVER_MAJOR_VERSION
            = new PropertyEntry("driverMajorVersion", Integer.MAX_VALUE, String.valueOf(Product.MAJOR_VERSION), "Driver major version.");

    public static final PropertyEntry PROPERTY_ENTRY$DRIVER_MINOR_VERSION
            = new PropertyEntry("driverMinorVersion", Integer.MAX_VALUE, String.valueOf(Product.MINOR_VERSION), "Driver minor version.");

    public static final PropertyEntry PROPERTY_ENTRY$META_DATA_TTL
            = new PropertyEntry("metaDataTTL", Integer.MAX_VALUE, String.valueOf(5 * 60), "Database meta data ttl(min)");

    // =======================================================

    static final int RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;

    static final int RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;

    static final int RESULT_SET_HOLDABILITY = ResultSet.CLOSE_CURSORS_AT_COMMIT;

    static final int RESULT_SET_FETCH_DIRECTION = ResultSet.FETCH_FORWARD;

    private static ReadonlyList<PropertyEntry> propertyEntryList;

    public static ReadonlyList<PropertyEntry> getPropertyEntryList() {
        if (Protocol.propertyEntryList == null) {
            synchronized (Protocol.class) {
                if (Protocol.propertyEntryList == null) {
                    final ArrayList<PropertyEntry> propertyEntryList = new ArrayList<PropertyEntry>();
                    for (Field field : Protocol.class.getDeclaredFields()) {
                        final int fieldModifier = field.getModifiers();
                        if (Modifier.isPublic(fieldModifier)
                                && Modifier.isStatic(fieldModifier)
                                && Modifier.isFinal(fieldModifier)
                                && field.getType() == PropertyEntry.class) {
                            try {
                                final PropertyEntry propertyEntry = (PropertyEntry) field.get(null);
                                if (propertyEntry != null) {
                                    propertyEntryList.add(propertyEntry);
                                }
                            } catch (IllegalArgumentException exception) {
                                // to do nothing.
                            } catch (IllegalAccessException exception) {
                                // to do nothing.
                            }
                        }
                    }
                    Protocol.propertyEntryList = ReadonlyList.newInstance(propertyEntryList);
                }
            }
        }
        return Protocol.propertyEntryList;
    }

    public static final class PropertyEntry {

        PropertyEntry(final String identifier,
                      final int maxinumValueLength,
                      final String defaultValue,
                      final String description) {
            this.identifier = identifier;
            this.maxinumValueLength = maxinumValueLength;
            this.defaultValue = defaultValue;
            this.description = defaultValue;
        }

        public final String identifier;

        public final int maxinumValueLength;

        public final String defaultValue;

        public final String description;

    }


}
