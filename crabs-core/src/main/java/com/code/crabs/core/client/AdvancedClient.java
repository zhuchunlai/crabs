package com.code.crabs.core.client;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.common.util.StringUtils;
import com.code.crabs.core.Identifier;
import com.code.crabs.core.IndexDefinition;
import com.code.crabs.core.TypeDefinition;
import com.code.crabs.exception.crabsException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.action.admin.indices.IndicesAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * 与elasticsearch交互的客户端
 *
 * @author zhuchunlai
 * @version $Id: AdvancedClient.java, v1.0 2014/07/31 11:15 $
 */
public final class AdvancedClient implements Closeable {

    private static final Properties EMPTY_PROPERTIES = new Properties();

    private final Client physicalClient;

    private final Properties properties;

    private final IndexDefinitionManager indexDefinitionManager;

    private final TypeDefinitionManager typeDefinitionManager;

    public AdvancedClient(final ElasticsearchAddress[] elasticsearchAddresses) {
        this(elasticsearchAddresses, EMPTY_PROPERTIES);
    }

    public AdvancedClient(final ElasticsearchAddress[] elasticsearchAddresses, final Properties properties) {
        ImmutableSettings.Builder builder = settingsBuilder();
        final Properties finallyProperties;
        if (properties == null || properties.isEmpty()) {
            finallyProperties = EMPTY_PROPERTIES;
        } else {
            finallyProperties = new Properties();
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                builder.put(entry.getKey().toString(), entry.getValue());
                finallyProperties.put(entry.getKey(), entry.getValue());
            }
        }
        final TransportClient client = new TransportClient(builder.build());
        for (ElasticsearchAddress address : elasticsearchAddresses) {
            client.addTransportAddress(new InetSocketTransportAddress(address.host, address.port));
        }
        this.physicalClient = client;
        this.properties = finallyProperties;
        this.indexDefinitionManager = new IndexDefinitionManager(this);
        this.typeDefinitionManager = new TypeDefinitionManager(this);
    }

    public final Object getProperty(final String propertyName, final Object defaultValue) {
        final Object propertyValue = this.properties.get(propertyName);
        return propertyValue == null ? defaultValue : propertyValue;
    }

    public final Properties cloneProperties() {
        final Properties properties = new Properties();
        for (Map.Entry<Object, Object> entry : this.properties.entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    public final void createIndex(final IndexDefinition indexDefinition) throws crabsException {
        if (indexDefinition == null) {
            throw new IllegalArgumentException("Argument[indexDefinition] is null.");
        }
        this.indexDefinitionManager.createIndex(indexDefinition);
    }

    public final void dropIndex(final Identifier identifier) throws crabsException {
        if (identifier == null) {
            throw new IllegalArgumentException("Argument[identifier] is null.");
        }
        this.indexDefinitionManager.dropIndex(identifier);
    }

    public final boolean existsIndex(final Identifier identifier) throws crabsException {
        if (identifier == null) {
            throw new IllegalArgumentException("Argument[identifier] is null.");
        }
        return this.indexDefinitionManager.exists(identifier);
    }

    public final IndexDefinition getIndexDefinition(final Identifier identifier) throws crabsException {
        if (identifier == null) {
            throw new IllegalArgumentException("Argument[identifier] is null.");
        }
        return this.indexDefinitionManager.getIndexDefinition(identifier);
    }

    public final ReadonlyList<IndexDefinition> getAllIndices() throws crabsException {
        return this.indexDefinitionManager.getAllIndices();
    }

    public final void createType(final TypeDefinition typeDefinition) throws crabsException {
        if (typeDefinition == null) {
            throw new IllegalArgumentException("Argument[typeDefinition] is null.");
        }
        this.typeDefinitionManager.createType(typeDefinition);
    }

    public final void dropType(final Identifier indexIdentifier,
                               final Identifier typeIdentifier) throws crabsException {
        if (indexIdentifier == null) {
            throw new IllegalArgumentException("Argument[indexIdentifier] is null.");
        }
        if (typeIdentifier == null) {
            throw new IllegalArgumentException("Argument[typeIdentifier] is null.");
        }
        final TypeDefinition typeDefinition = new TypeDefinition(
                new IndexDefinition(indexIdentifier),
                typeIdentifier
        );
        this.typeDefinitionManager.dropType(typeDefinition);
    }

    public final void dropType(final TypeDefinition typeDefinition) throws crabsException {
        if (typeDefinition == null) {
            throw new IllegalArgumentException("Argument[typeDefinition] is null.");
        }
        this.typeDefinitionManager.dropType(typeDefinition);
    }

    public final boolean existsType(final Identifier indexIdentifier,
                                    final Identifier typeIdentifier) throws crabsException {
        if (indexIdentifier == null) {
            throw new IllegalArgumentException("Argument[indexIdentifier] is null.");
        }
        if (typeIdentifier == null) {
            throw new IllegalArgumentException("Argument[typeIdentifier] is null.");
        }
        final TypeDefinition typeDefinition = new TypeDefinition(
                new IndexDefinition(indexIdentifier),
                typeIdentifier
        );
        return this.typeDefinitionManager.exists(typeDefinition);
    }

    public final boolean existsType(final TypeDefinition typeDefinition) throws crabsException {
        if (typeDefinition == null) {
            throw new IllegalArgumentException("Argument[typeDefinition] is null.");
        }
        return this.typeDefinitionManager.exists(typeDefinition);
    }

    public final TypeDefinition getTypeDefinition(final Identifier indexIdentifier,
                                                  final Identifier typeIdentifier) throws crabsException {
        if (indexIdentifier == null) {
            throw new IllegalArgumentException("Argument[indexIdentifier] is null.");
        }
        if (typeIdentifier == null) {
            throw new IllegalArgumentException("Argument[typeIdentifier] is null.");
        }
        final IndexDefinition indexDefinition = this.getIndexDefinition(indexIdentifier);
        return this.typeDefinitionManager.getTypeDefinition(indexDefinition, typeIdentifier);
    }

    public final TypeDefinition getTypeDefinition(final IndexDefinition indexDefinition,
                                                  final Identifier typeIdentifier) throws crabsException {
        if (indexDefinition == null) {
            throw new IllegalArgumentException("Argument[indexDefinition] is null.");
        }
        if (typeIdentifier == null) {
            throw new IllegalArgumentException("Argument[typeIdentifier] is null.");
        }
        return this.typeDefinitionManager.getTypeDefinition(indexDefinition, typeIdentifier);
    }

    public final ReadonlyList<TypeDefinition> getTypeDefinitions(
            final Identifier indexIdentifier) throws crabsException {
        if (indexIdentifier == null) {
            throw new IllegalArgumentException("Argument[indexIdentifier] is null.");
        }
        return this.typeDefinitionManager.getTypeDefinitions(indexIdentifier);
    }

    public final <Request extends ActionRequest,
            Response extends ActionResponse,
            Builder extends ActionRequestBuilder<Request, Response, Builder>,
            TAction extends Action<Request, Response, Builder>, V> void execute(final InternalDocumentRequestBuilder<Request, Response, Builder, TAction, V> internalDocumentRequestBuilder,
                                                                                final ResponseCallback<Response> callback,
                                                                                final V value) throws crabsException {
        final Response response = this.physicalClient.execute(
                internalDocumentRequestBuilder.buildAction(),
                internalDocumentRequestBuilder.buildRequest(this.physicalClient, value)
        ).actionGet();
        callback.callback(response);
    }

    public final <Request extends ActionRequest,
            Response extends ActionResponse,
            Builder extends ActionRequestBuilder<Request, Response, Builder>,
            Action extends IndicesAction<Request, Response, Builder>, V> void execute(final InternalIndicesRequestBuilder<Request, Response, Builder, Action, V> internalIndicesRequestBuilder,
                                                                                      final ResponseCallback<Response> callback,
                                                                                      final V value) throws crabsException {
        final IndicesAdminClient adminClient = this.physicalClient.admin().indices();
        final Response response = adminClient.execute(
                internalIndicesRequestBuilder.buildAction(),
                internalIndicesRequestBuilder.buildRequest(adminClient, value)
        ).actionGet();
        callback.callback(response);
    }

    public final <Request extends ActionRequest,
            Response extends ActionResponse,
            Builder extends ActionRequestBuilder<Request, Response, Builder>,
            Action extends ClusterAction<Request, Response, Builder>, V> void execute(final InternalClusterRequestBuilder<Request, Response, Builder, Action, V> abstractIndicesRequestBuilder,
                                                                                      final ResponseCallback<Response> callback,
                                                                                      final V value) throws crabsException {
        final ClusterAdminClient adminClient = this.physicalClient.admin().cluster();
        final Response response = adminClient.execute(
                abstractIndicesRequestBuilder.buildAction(),
                abstractIndicesRequestBuilder.buildRequest(adminClient, value)
        ).actionGet();
        callback.callback(response);
    }


    @Override
    public final void close() throws IOException {
        this.physicalClient.close();
    }

    public static final class ElasticsearchAddress {

        private static final String DELIMITER_BETWEEN_ADDRESSES = ",";

        private static final String DELIMITER_BETWEEN_HOST_PORT = ":";

        private final String host;

        private final int port;

        public ElasticsearchAddress(final String host, final int port) {
            if (StringUtils.isNullOrEmptyAfterTrim(host)) {
                throw new IllegalArgumentException("Argument[host] is required.");
            }
            if (port <= 0) {
                throw new IllegalArgumentException("Argument[port] is must greater than zero.");
            }
            this.host = host;
            this.port = port;
        }

        public static ElasticsearchAddress[] toElasticsearchAddresses(final String URL) {
            if (StringUtils.isNullOrEmptyAfterTrim(URL)) {
                throw new IllegalArgumentException("Argument[URL] is null or empty.");
            }
            final String[] addresses = URL.split(DELIMITER_BETWEEN_ADDRESSES);
            final int addressCount = addresses.length;
            final ElasticsearchAddress[] finallyAddresses = new ElasticsearchAddress[addressCount];
            for (int index = 0; index < addressCount; index++) {
                finallyAddresses[index] = new ElasticsearchAddress(addresses[index]);
            }
            return finallyAddresses;
        }

        public ElasticsearchAddress(final String address) {
            if (StringUtils.isNullOrEmptyAfterTrim(address)) {
                throw new IllegalArgumentException("Argument[address] is null or empty.");
            }
            final String[] values = address.split(DELIMITER_BETWEEN_HOST_PORT);
            if (values.length != 2) {
                throw new IllegalArgumentException("Argument[address] is invalid, expected format is: [host:port]");
            }
            final String host = values[0];
            final int port;
            try {
                port = Integer.parseInt(values[1]);
            } catch (Exception e) {
                throw new IllegalArgumentException("Argument[address] is invalid, expected format is: [host:port]");
            }
            if (StringUtils.isNullOrEmptyAfterTrim(host)) {
                throw new IllegalArgumentException("Argument[address] is invalid, expected format is: [host:port]");
            }
            this.host = host.trim();
            this.port = port;
        }

        private String toStringValue;

        @Override
        public final String toString() {
            if (this.toStringValue == null) {
                this.toStringValue = this.host + DELIMITER_BETWEEN_HOST_PORT + this.port;
            }
            return this.toStringValue;
        }

        @Override
        public final boolean equals(final Object obj) {
            return obj != null && obj instanceof ElasticsearchAddress
                    && this.host.equals(((ElasticsearchAddress) obj).host)
                    && this.port == ((ElasticsearchAddress) obj).port;
        }

    }

    public interface InternalDocumentRequestBuilder<Request extends ActionRequest, Response extends ActionResponse,
            Builder extends ActionRequestBuilder<Request, Response, Builder>,
            TAction extends Action<Request, Response, Builder>, V> {

        TAction buildAction();

        Request buildRequest(Client client, V value) throws crabsException;

    }

    public interface InternalIndicesRequestBuilder<Request extends ActionRequest, Response extends ActionResponse,
            Builder extends ActionRequestBuilder<Request, Response, Builder>,
            Action extends IndicesAction<Request, Response, Builder>, V> {

        Action buildAction();

        Request buildRequest(IndicesAdminClient adminClient, V value) throws crabsException;
    }

    public interface InternalClusterRequestBuilder<Request extends ActionRequest, Response extends ActionResponse,
            Builder extends ActionRequestBuilder<Request, Response, Builder>,
            Action extends ClusterAction<Request, Response, Builder>, V> {

        Action buildAction();

        Request buildRequest(ClusterAdminClient adminClient, V value) throws crabsException;
    }


    public interface ResponseCallback<T extends ActionResponse> {
        void callback(T response) throws crabsException;
    }

}
