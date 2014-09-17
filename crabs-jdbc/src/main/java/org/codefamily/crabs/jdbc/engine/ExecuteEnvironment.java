package org.codefamily.crabs.jdbc.engine;

import org.codefamily.crabs.common.util.TimeCacheMap;
import org.codefamily.crabs.core.Identifier;
import org.codefamily.crabs.core.IndexDefinition;
import org.codefamily.crabs.core.TypeDefinition;
import org.codefamily.crabs.core.client.AdvancedClient;
import org.codefamily.crabs.exception.SQL4ESException;
import org.codefamily.crabs.jdbc.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public final class ExecuteEnvironment implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ExecuteEnvironment.class);

    private static final Properties EMPTY_PROPERTIES = new Properties();

    private final AdvancedClient advancedClient;

    private final Identifier indexIdentifier;

    private final Properties properties;

    private final TimeCacheMap<Identifier, TypeDefinition> typeDefinitionCache;

    public ExecuteEnvironment(final AdvancedClient advancedClient,
                              final Identifier indexIdentifier) {
        this(advancedClient, indexIdentifier, EMPTY_PROPERTIES);
    }

    public ExecuteEnvironment(final AdvancedClient advancedClient,
                              final Identifier indexIdentifier,
                              final Properties properties) {
        if (advancedClient == null) {
            throw new IllegalArgumentException("Argument[advancedClient] is null.");
        }
        if (indexIdentifier == null) {
            throw new IllegalArgumentException("Argument[indexIdentifier] is null.");
        }
        final Properties finallyProperties = new Properties();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            finallyProperties.put(entry.getKey(), entry.getValue());
        }
        this.advancedClient = advancedClient;
        this.indexIdentifier = indexIdentifier;
        this.properties = finallyProperties;
        this.closed = false;
        this.typeDefinitionCache = new TimeCacheMap<Identifier, TypeDefinition>(
                Integer.parseInt(
                        this.getProperty(
                                Protocol.PROPERTY_ENTRY$META_DATA_TTL.identifier,
                                Protocol.PROPERTY_ENTRY$META_DATA_TTL.defaultValue
                        )
                ),
                10,
                new TimeCacheMap.ExpiredCallback<Identifier, TypeDefinition>() {
                    @Override
                    public final void expire(final Identifier typeIdentifier,
                                             final TypeDefinition typeDefinition) {
                        // nothing to do.
                    }
                }
        );
    }

    public final String getProperty(final String propertyName, final String defaultValue) {
        return this.properties.getProperty(propertyName, defaultValue);
    }

    private volatile boolean closed;

    @Override
    public void close() throws IOException {
        if (!this.closed) {
            synchronized (this) {
                if (!this.closed) {
                    this.closed = true;
                    this.typeDefinitionCache.cleanup();
                }
            }
        }
    }

    private IndexDefinition indexDefinition;

    public final IndexDefinition getIndexDefinition() throws SQL4ESException {
        if (this.indexDefinition == null) {
            synchronized (this) {
                if (this.indexDefinition == null) {
                    this.indexDefinition = this.advancedClient.getIndexDefinition(this.indexIdentifier);
                }
            }
        }
        return this.indexDefinition;
    }

    public final TypeDefinition getTypeDefinition(final Identifier typeIdentifier) throws SQL4ESException {
        TypeDefinition typeDefinition = this.typeDefinitionCache.get(typeIdentifier);
        if (typeDefinition == null) {
            synchronized (this) {
                typeDefinition = this.typeDefinitionCache.get(typeIdentifier);
                if (typeDefinition == null) {
                    typeDefinition = this.advancedClient.getTypeDefinition(this.indexIdentifier, typeIdentifier);
                    this.typeDefinitionCache.put(typeIdentifier, typeDefinition);
                }
            }
        }
        return typeDefinition;
    }

    // ----------------- 以下是性能测试所需 -----------------

    private final ThreadLocal<Long> startTimeInMillisThreadLocal = new ThreadLocal<Long>();

    private long totalCostTimeInMillis = 0;

    public final void start() {
        this.startTimeInMillisThreadLocal.set(System.currentTimeMillis());
    }

    public final void end() {
        final Long startTimeInMillis = this.startTimeInMillisThreadLocal.get();
        if (startTimeInMillis == null) {
            throw new IllegalStateException("Illegal state, for there's no start time.");
        }
        final long cost = System.currentTimeMillis() - startTimeInMillis;
        this.totalCostTimeInMillis += cost;
    }

    public final long getTotalCostTimeInMillis() {
        return this.totalCostTimeInMillis;
    }

}
