package com.code.crabs.core.client;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.core.Identifier;
import com.code.crabs.core.IndexDefinition;
import com.code.crabs.core.client.AdvancedClient.InternalClusterRequestBuilder;
import com.code.crabs.core.client.AdvancedClient.InternalIndicesRequestBuilder;
import com.code.crabs.core.exception.IndexAlreadyExistsException;
import com.code.crabs.core.exception.IndexNotExistsException;
import com.code.crabs.exception.crabsException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static com.code.crabs.common.Constants.FIELD_INDEX_REPLICAS_NUM;
import static com.code.crabs.common.Constants.FIELD_INDEX_SHARDS_NUM;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * elasticsearch index 管理器，和elasticsearch交互，实现index的DDL操作
 *
 * @author zhuchunlai
 * @version $Id: IndexDefinitionManager.java, v1.0 2014/07/30 16:56 $
 */
final class IndexDefinitionManager {

    private final AdvancedClient advancedClient;

    IndexDefinitionManager(final AdvancedClient advancedClient) {
        this.advancedClient = advancedClient;
    }

    final void createIndex(final IndexDefinition indexDefinition) throws crabsException {
        final class InternalIndicesRequestBuilder$CreateIndex implements
                InternalIndicesRequestBuilder<CreateIndexRequest, CreateIndexResponse, CreateIndexRequestBuilder, CreateIndexAction, IndexDefinition> {
            @Override
            public final CreateIndexAction buildAction() {
                return CreateIndexAction.INSTANCE;
            }

            @Override
            public final CreateIndexRequest buildRequest(final IndicesAdminClient adminClient,
                                                         final IndexDefinition indexDefinition) throws crabsException {
                final CreateIndexRequestBuilder builder = new CreateIndexRequestBuilder(adminClient, indexDefinition.getIdentifier().toString());
                XContentBuilder settings;
                try {
                    settings = jsonBuilder()
                            .startObject()
                            .field(FIELD_INDEX_SHARDS_NUM, indexDefinition.getShardsNum())
                            .field(FIELD_INDEX_REPLICAS_NUM, indexDefinition.getReplicasNum())
                            .endObject();
                } catch (IOException e) {
                    throw new crabsException(e);
                }
                builder.setSettings(settings);
                return builder.request();
            }
        }
        try {
            if (this.exists(indexDefinition.getIdentifier())) {
                throw new IndexAlreadyExistsException(indexDefinition);
            }
        } catch (Exception e) {
            throw new crabsException(e);
        }
        this.advancedClient.execute(
                new InternalIndicesRequestBuilder$CreateIndex(),
                new AdvancedClient.ResponseCallback<CreateIndexResponse>() {
                    @Override
                    public final void callback(final CreateIndexResponse response) throws crabsException {
                        // nothing to do.
                    }
                },
                indexDefinition
        );
    }

    final void alterIndex(final IndexDefinition indexDefinition) {
        throw new UnsupportedOperationException();
    }

    final void dropIndex(final Identifier identifier) throws crabsException {
        final class InternalIndicesRequestBuilder$DeleteIndex implements
                InternalIndicesRequestBuilder<DeleteIndexRequest, DeleteIndexResponse,
                        DeleteIndexRequestBuilder, DeleteIndexAction, String> {
            @Override
            public final DeleteIndexAction buildAction() {
                return DeleteIndexAction.INSTANCE;
            }

            @Override
            public final DeleteIndexRequest buildRequest(final IndicesAdminClient adminClient,
                                                         final String identifier) throws crabsException {
                final DeleteIndexRequestBuilder builder
                        = new DeleteIndexRequestBuilder(adminClient, identifier);
                return builder.request();
            }
        }
        final String indexName = identifier.toString();
        try {
            this.advancedClient.execute(
                    new InternalIndicesRequestBuilder$DeleteIndex(),
                    new AdvancedClient.ResponseCallback<DeleteIndexResponse>() {
                        @Override
                        public final void callback(final DeleteIndexResponse response) throws crabsException {
                            // nothing to do.
                        }
                    },
                    indexName
            );
        } catch (IndexMissingException e) {
            throw new IndexNotExistsException(indexName);
        }
    }

    final ReadonlyList<IndexDefinition> getAllIndices() throws crabsException {
        final class InternalClusterRequestBuilder$ClusterState implements
                InternalClusterRequestBuilder<ClusterStateRequest, ClusterStateResponse, ClusterStateRequestBuilder, ClusterStateAction, Object> {
            @Override
            public final ClusterStateAction buildAction() {
                return ClusterStateAction.INSTANCE;
            }

            @Override
            public final ClusterStateRequest buildRequest(final ClusterAdminClient adminClient,
                                                          final Object value) throws crabsException {
                final ClusterStateRequestBuilder builder = new ClusterStateRequestBuilder(adminClient);
                return builder.request();
            }
        }
        final ArrayList<IndexDefinition> indexDefinitionList = new ArrayList<IndexDefinition>();
        this.advancedClient.execute(
                new InternalClusterRequestBuilder$ClusterState(),
                new AdvancedClient.ResponseCallback<ClusterStateResponse>() {
                    @Override
                    public final void callback(final ClusterStateResponse response) throws crabsException {
                        final MetaData clusterMetaData = response.getState().getMetaData();
                        final ImmutableOpenMap<String, IndexMetaData> indexMetaDataMap = clusterMetaData.indices();
                        IndexMetaData indexMetaData;
                        for (Iterator<IndexMetaData> iterator = indexMetaDataMap.valuesIt(); iterator.hasNext(); ) {
                            indexMetaData = iterator.next();
                            indexDefinitionList.add(
                                    new IndexDefinition(
                                            new Identifier(indexMetaData.index()),
                                            indexMetaData.getNumberOfShards(),
                                            indexMetaData.getNumberOfReplicas()
                                    )
                            );
                        }
                    }
                },
                null
        );
        return ReadonlyList.newInstance(indexDefinitionList);
    }

    final IndexDefinition getIndexDefinition(final Identifier indexIdentifier) throws crabsException {
        try {
            if (!this.exists(indexIdentifier)) {
                throw new IndexNotExistsException("Index[" + indexIdentifier + "] is not exists.");
            }
        } catch (Exception e) {
            throw new crabsException(e);
        }
        final class InternalClusterRequestBuilder$ClusterState implements
                InternalClusterRequestBuilder<ClusterStateRequest, ClusterStateResponse, ClusterStateRequestBuilder, ClusterStateAction, Identifier> {
            @Override
            public final ClusterStateAction buildAction() {
                return ClusterStateAction.INSTANCE;
            }

            @Override
            public final ClusterStateRequest buildRequest(final ClusterAdminClient adminClient,
                                                          final Identifier indexIdentifier) throws crabsException {
                final ClusterStateRequestBuilder builder = new ClusterStateRequestBuilder(adminClient);
                builder.setIndices(indexIdentifier.toString());
                return builder.request();
            }
        }
        final ArrayList<IndexDefinition> indexDefinitionList = new ArrayList<IndexDefinition>(1);
        this.advancedClient.execute(
                new InternalClusterRequestBuilder$ClusterState(),
                new AdvancedClient.ResponseCallback<ClusterStateResponse>() {
                    @Override
                    public final void callback(final ClusterStateResponse response) throws crabsException {
                        final MetaData clusterMetaData = response.getState().getMetaData();
                        final ImmutableOpenMap<String, IndexMetaData> indexMetaDataMap = clusterMetaData.indices();
                        IndexMetaData indexMetaData;
                        for (Iterator<IndexMetaData> iterator = indexMetaDataMap.valuesIt(); iterator.hasNext(); ) {
                            indexMetaData = iterator.next();
                            indexDefinitionList.add(
                                    new IndexDefinition(
                                            new Identifier(indexMetaData.index()),
                                            indexMetaData.getNumberOfShards(),
                                            indexMetaData.getNumberOfReplicas()
                                    )
                            );
                        }
                    }
                },
                indexIdentifier
        );
        if (indexDefinitionList.isEmpty()) {
            throw new IndexNotExistsException("Index[" + indexIdentifier.toString() + "] is not exists.");
        }
        return indexDefinitionList.get(0);
    }

    final boolean exists(final Identifier indexIdentifier) throws crabsException {
        final class InternalIndicesRequestBuilder$IndexExists implements
                InternalIndicesRequestBuilder<IndicesExistsRequest, IndicesExistsResponse, IndicesExistsRequestBuilder, IndicesExistsAction, Identifier> {
            @Override
            public final IndicesExistsAction buildAction() {
                return IndicesExistsAction.INSTANCE;
            }

            @Override
            public final IndicesExistsRequest buildRequest(final IndicesAdminClient adminClient,
                                                           final Identifier value) throws crabsException {
                IndicesExistsRequestBuilder builder = new IndicesExistsRequestBuilder(adminClient, value.toString());
                return builder.request();
            }
        }
        final class Result {
            private boolean isExists;
        }
        final Result result = new Result();
        this.advancedClient.execute(
                new InternalIndicesRequestBuilder$IndexExists(),
                new AdvancedClient.ResponseCallback<IndicesExistsResponse>() {
                    @Override
                    public final void callback(final IndicesExistsResponse response) throws crabsException {
                        result.isExists = response.isExists();
                    }
                },
                indexIdentifier
        );
        return result.isExists;
    }

}
