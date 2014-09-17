package org.codefamily.crabs.core.client;

import org.codefamily.crabs.common.Constants;
import org.codefamily.crabs.common.util.ReadonlyList;
import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.core.Identifier;
import org.codefamily.crabs.core.IndexDefinition;
import org.codefamily.crabs.core.TypeDefinition;
import org.codefamily.crabs.core.client.AdvancedClient.InternalIndicesRequestBuilder;
import org.codefamily.crabs.core.exception.*;
import org.codefamily.crabs.exception.SQL4ESException;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingAction;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.internal.InternalGenericClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.TypeMissingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * elasticsearch type 管理器，和elasticsearch交互，实现type的DDL操作
 *
 * @author zhuchunlai
 * @version $Id: TypeDefinitionManager.java, v1.0 2014/07/30 17:02 $
 */
final class TypeDefinitionManager {

    private final AdvancedClient advancedClient;

    TypeDefinitionManager(final AdvancedClient advancedClient) {
        this.advancedClient = advancedClient;
    }

    final void createType(final TypeDefinition typeDefinition) throws SQL4ESException {
        final class InternalIndicesRequestBuilder$CreateType implements
                InternalIndicesRequestBuilder<PutMappingRequest, PutMappingResponse, PutMappingRequestBuilder, PutMappingAction, TypeDefinition> {
            @Override
            public final PutMappingAction buildAction() {
                return PutMappingAction.INSTANCE;
            }

            @Override
            public final PutMappingRequest buildRequest(final IndicesAdminClient adminClient,
                                                        final TypeDefinition typeDefinition) throws SQL4ESException {
                final PutMappingRequestBuilder builder = new PutMappingRequestBuilder(adminClient);
                builder.setIndices(typeDefinition.getIndexDefinition().getIdentifier().toString());
                builder.setType(typeDefinition.getIdentifier().toString());
                XContentBuilder contentBuilder;
                try {
                    contentBuilder = jsonBuilder();
                    contentBuilder.startObject();
                    contentBuilder.startObject("_id")
                            .field("path", typeDefinition.getPrimaryFieldDefinition().getIdentifier().toString())
                            .endObject();
                    contentBuilder.startObject("_all").field("enabled", typeDefinition.isAllEnabled()).endObject();
                    contentBuilder.startObject("_ttl").field("enabled", typeDefinition.isTTLEnabled()).endObject();
                    contentBuilder.startObject("properties");
                    for (TypeDefinition.FieldDefinition fieldDefinition : typeDefinition.getAllFieldDefinitions()) {
                        final DataType dataType = fieldDefinition.getDataType();
                        contentBuilder.startObject(fieldDefinition.getIdentifier().toString());
                        contentBuilder.field("type", dataType.getElasticsearchType());
                        switch (dataType) {
                            case DATE:
                                contentBuilder.field("format", fieldDefinition.getPattern());
                                break;
                            case STRING:
                                contentBuilder.field("index", "not_analyzed");
                                break;
                        }
                        contentBuilder.endObject();
                    }
                    contentBuilder.endObject();
                    contentBuilder.endObject();
                } catch (IOException e) {
                    throw new SQL4ESException(e);
                }
                try {
                    System.out.println(contentBuilder.string());
                } catch (IOException e) {
                    // nothing to do.
                }
                builder.setSource(contentBuilder);
                return builder.request();
            }
        }
        try {
            if (this.exists(typeDefinition)) {
                throw new TypeAlreadyExistsException(typeDefinition);
            }
            this.advancedClient.execute(
                    new InternalIndicesRequestBuilder$CreateType(),
                    new AdvancedClient.ResponseCallback<PutMappingResponse>() {
                        @Override
                        public final void callback(final PutMappingResponse response) throws SQL4ESException {
                            // nothing to do.
                        }
                    },
                    typeDefinition
            );
        } catch (IndexMissingException e) {
            throw new SQL4ESException(new IndexNotExistsException(typeDefinition.getIndexDefinition()));
        } catch (Exception e) {
            throw new SQL4ESException(e);
        }
    }

    final void alterType(final TypeDefinition typeDefinition) {
        throw new UnsupportedOperationException();
    }

    final void dropType(final TypeDefinition typeDefinition) throws SQL4ESException {
        final class InternalIndicesRequestBuilder$DeleteType implements
                InternalIndicesRequestBuilder<DeleteMappingRequest, DeleteMappingResponse, DeleteMappingRequestBuilder, DeleteMappingAction, TypeDefinition> {

            @Override
            public final DeleteMappingAction buildAction() {
                return DeleteMappingAction.INSTANCE;
            }

            @Override
            public final DeleteMappingRequest buildRequest(final IndicesAdminClient adminClient,
                                                           final TypeDefinition typeDefinition) throws SQL4ESException {
                final DeleteMappingRequestBuilder builder = new DeleteMappingRequestBuilder(adminClient);
                builder.setIndices(typeDefinition.getIndexDefinition().getIdentifier().toString());
                builder.setType(typeDefinition.getIdentifier().toString());
                return builder.request();
            }
        }
        try {
            this.advancedClient.execute(
                    new InternalIndicesRequestBuilder$DeleteType(),
                    new AdvancedClient.ResponseCallback<DeleteMappingResponse>() {
                        @Override
                        public void callback(DeleteMappingResponse response) throws SQL4ESException {
                            // nothing to do.
                        }
                    },
                    typeDefinition
            );
        } catch (IndexMissingException e) {
            throw new IndexNotExistsException("Index[" + typeDefinition.getIndexDefinition().getIdentifier().toString() + "] is not exists.");
        } catch (TypeMissingException e) {
            throw new TypeNotExistsException(typeDefinition);
        }
    }

    final ReadonlyList<TypeDefinition> getTypeDefinitions(final Identifier indexIdentifier) throws SQL4ESException {
        final class InternalIndicesRequestBuilder$GetMappings implements
                InternalIndicesRequestBuilder<GetMappingsRequest, GetMappingsResponse, GetMappingsRequestBuilder, GetMappingsAction, Identifier> {

            @Override
            public final GetMappingsAction buildAction() {
                return GetMappingsAction.INSTANCE;
            }

            @Override
            public final GetMappingsRequest buildRequest(final IndicesAdminClient adminClient,
                                                         final Identifier indexIdentifier) throws SQL4ESException {
                final GetMappingsRequestBuilder builder = new GetMappingsRequestBuilder(
                        (InternalGenericClient) adminClient,
                        indexIdentifier.toString()
                );
                return builder.request();
            }
        }
        final IndexDefinitionManager indexDefinitionManager = new IndexDefinitionManager(this.advancedClient);
        final IndexDefinition indexDefinition = indexDefinitionManager.getIndexDefinition(indexIdentifier);
        final ArrayList<TypeDefinition> typeDefinitionList = new ArrayList<TypeDefinition>();
        try {
            this.advancedClient.execute(
                    new InternalIndicesRequestBuilder$GetMappings(),
                    new AdvancedClient.ResponseCallback<GetMappingsResponse>() {
                        @Override
                        public final void callback(final GetMappingsResponse response) throws SQL4ESException {
                            final ImmutableOpenMap<String, MappingMetaData> typeMappingMetaDataMap
                                    = response.mappings().get(indexIdentifier.toString());
                            MappingMetaData mappingMetaData;
                            TypeDefinition typeDefinition;
                            Map<String, Object> source;
                            boolean allEnabled = true, ttlEnabled = false;
                            Identifier primaryFieldIdentifier;
                            TypeDefinition.FieldDefinition fieldDefinition;
                            for (Iterator<MappingMetaData> iterator = typeMappingMetaDataMap.valuesIt(); iterator.hasNext(); ) {
                                mappingMetaData = iterator.next();
                                try {
                                    source = mappingMetaData.sourceAsMap();
                                } catch (IOException e) {
                                    throw new SQL4ESException(e);
                                }
                                if (source.containsKey("_all")) {
                                    @SuppressWarnings("unchecked")
                                    final LinkedHashMap<String, Object> allValueMap
                                            = (LinkedHashMap<String, Object>) source.get("_all");
                                    final Object allEnabledValue = allValueMap.get("_enabled");
                                    allEnabled = allEnabledValue != null && Boolean.parseBoolean(allEnabledValue.toString());
                                }
                                if (source.containsKey("_ttl")) {
                                    @SuppressWarnings("unchecked")
                                    final LinkedHashMap<String, Object> ttlValueMap
                                            = (LinkedHashMap<String, Object>) source.get("_ttl");
                                    final Object ttlEnabledValue = ttlValueMap.get("_enabled");
                                    ttlEnabled = ttlEnabledValue != null && Boolean.parseBoolean(ttlEnabledValue.toString());
                                }
                                typeDefinition = new TypeDefinition(
                                        indexDefinition,
                                        new Identifier(mappingMetaData.type()),
                                        ttlEnabled,
                                        allEnabled
                                );
                                if (!source.containsKey("_id")) {
                                    throw new PrimaryFieldNotFoundException(typeDefinition);
                                }
                                @SuppressWarnings("unchecked")
                                final LinkedHashMap<String, Object> primaryFieldMap = (LinkedHashMap<String, Object>) source.get("_id");
                                primaryFieldIdentifier = new Identifier(primaryFieldMap.get("path").toString());
                                @SuppressWarnings("unchecked")
                                final LinkedHashMap<String, Object> properties
                                        = (LinkedHashMap<String, Object>) source.get("properties");
                                for (Map.Entry<String, Object> fieldEntry : properties.entrySet()) {
                                    final String fieldName = fieldEntry.getKey();
                                    @SuppressWarnings("unchecked")
                                    final LinkedHashMap<String, Object> fieldProperties
                                            = (LinkedHashMap<String, Object>) fieldEntry.getValue();
                                    final String elasticsearchType = fieldProperties.get("type").toString();
                                    final DataType dataType = DataType.getDataTypeWithElasticsearchType(elasticsearchType);
                                    final String pattern;
                                    if (dataType == DataType.DATE) {
                                        final Object formatValue = fieldProperties.get("format");
                                        if (formatValue == null) {
                                            throw new SQL4ESException(
                                                    new FormatPatternNotFoundException(
                                                            "There's no format mapping for field[" +
                                                                    fieldName + "] in type[" +
                                                                    typeDefinition.getIdentifier() + "]"
                                                    )
                                            );
                                        }
                                        pattern = formatValue.toString();
                                    } else {
                                        pattern = null;
                                    }
                                    final Object storedValue = fieldProperties.get("stored");
                                    fieldDefinition = typeDefinition.defineField(
                                            new Identifier(fieldName),
                                            dataType,
                                            pattern,
                                            storedValue != null && Boolean.parseBoolean(storedValue.toString())
                                    );
                                    if (fieldDefinition.getIdentifier().equals(primaryFieldIdentifier)) {
                                        fieldDefinition.asPrimaryField();
                                    }
                                }
                                typeDefinition.publish();
                                typeDefinitionList.add(typeDefinition);
                            }
                        }
                    },
                    indexIdentifier
            );
        } catch (IndexMissingException e) {
            throw new IndexNotExistsException("Index[" + indexIdentifier.toString() + "] is not exists.");
        }
        return ReadonlyList.newInstance(typeDefinitionList);
    }

    final TypeDefinition getTypeDefinition(final IndexDefinition indexDefinition,
                                           final Identifier typeIdentifier) throws SQL4ESException {
        final class InternalIndicesRequestBuilder$GetMapping implements
                InternalIndicesRequestBuilder<GetMappingsRequest, GetMappingsResponse,
                        GetMappingsRequestBuilder, GetMappingsAction, String> {

            @Override
            public final GetMappingsAction buildAction() {
                return GetMappingsAction.INSTANCE;
            }

            @Override
            public final GetMappingsRequest buildRequest(final IndicesAdminClient adminClient,
                                                         final String indexIdentifier) throws SQL4ESException {
                final GetMappingsRequestBuilder builder = new GetMappingsRequestBuilder(
                        (InternalGenericClient) adminClient,
                        indexIdentifier
                );
                builder.setTypes(typeIdentifier.toString());
                return builder.request();
            }
        }
        final String indexName = indexDefinition.getIdentifier().toString();
        final ArrayList<TypeDefinition> typeDefinitionList = new ArrayList<TypeDefinition>(1);
        try {
            this.advancedClient.execute(
                    new InternalIndicesRequestBuilder$GetMapping(),
                    new AdvancedClient.ResponseCallback<GetMappingsResponse>() {
                        @Override
                        public final void callback(final GetMappingsResponse response) throws SQL4ESException {
                            final ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> typeMappingMetaDataMap
                                    = response.mappings();
                            if (typeMappingMetaDataMap.size() == 0) {
                                throw new TypeNotExistsException("Type[" + typeIdentifier.toString() + "] is not exists.");
                            }
                            final MappingMetaData mappingMetaData
                                    = typeMappingMetaDataMap.get(indexName).get(typeIdentifier.toString());
                            final Map<String, Object> source;
                            try {
                                source = mappingMetaData.sourceAsMap();
                            } catch (IOException e) {
                                throw new SQL4ESException(e);
                            }
                            boolean allEnabled = false;
                            if (source.containsKey("_all")) {
                                @SuppressWarnings("unchecked")
                                final LinkedHashMap<String, Object> allValueMap
                                        = (LinkedHashMap<String, Object>) source.get("_all");
                                final Object allEnabledValue = allValueMap.get("_enabled");
                                allEnabled = allEnabledValue != null && Boolean.parseBoolean(allEnabledValue.toString());
                            }
                            boolean ttlEnabled = false;
                            if (source.containsKey("_ttl")) {
                                @SuppressWarnings("unchecked")
                                final LinkedHashMap<String, Object> ttlValueMap
                                        = (LinkedHashMap<String, Object>) source.get("_ttl");
                                final Object ttlEnabledValue = ttlValueMap.get("_enabled");
                                ttlEnabled = ttlEnabledValue != null && Boolean.parseBoolean(ttlEnabledValue.toString());
                            }
                            final TypeDefinition typeDefinition = new TypeDefinition(
                                    indexDefinition,
                                    new Identifier(mappingMetaData.type()),
                                    ttlEnabled,
                                    allEnabled
                            );
                            if (!source.containsKey("_id")) {
                                throw new PrimaryFieldNotFoundException(typeDefinition);
                            }
                            @SuppressWarnings("unchecked")
                            final LinkedHashMap<String, Object> primaryFieldMap = (LinkedHashMap<String, Object>) source.get("_id");
                            final Identifier primaryFieldIdentifier = new Identifier(primaryFieldMap.get("path").toString());
                            @SuppressWarnings("unchecked")
                            final LinkedHashMap<String, Object> properties
                                    = (LinkedHashMap<String, Object>) source.get("properties");
                            TypeDefinition.FieldDefinition fieldDefinition;
                            for (Map.Entry<String, Object> fieldEntry : properties.entrySet()) {
                                final String fieldName = fieldEntry.getKey();
                                @SuppressWarnings("unchecked")
                                final LinkedHashMap<String, Object> fieldProperties
                                        = (LinkedHashMap<String, Object>) fieldEntry.getValue();
                                final String elasticsearchType = fieldProperties.get("type").toString();
                                final DataType dataType = DataType.getDataTypeWithElasticsearchType(elasticsearchType);
                                final String pattern;
                                if (dataType == DataType.DATE) {
                                    final Object formatValue = fieldProperties.get("format");
                                    if (formatValue == null) {
                                        throw new SQL4ESException(
                                                new FormatPatternNotFoundException(
                                                        "There's no format mapping for field[" +
                                                                fieldName + "] in type[" +
                                                                typeDefinition.getIdentifier() + "]"
                                                )
                                        );
                                    }
                                    pattern = formatValue.toString();
                                } else {
                                    pattern = null;
                                }
                                final Object storedValue = fieldProperties.get("stored");
                                fieldDefinition = typeDefinition.defineField(
                                        new Identifier(fieldName),
                                        dataType,
                                        pattern,
                                        storedValue != null && Boolean.parseBoolean(storedValue.toString())
                                );
                                if (fieldDefinition.getIdentifier().equals(primaryFieldIdentifier)) {
                                    fieldDefinition.asPrimaryField();
                                }
                            }
                            typeDefinition.publish();
                            typeDefinitionList.add(typeDefinition);
                        }
                    },
                    indexName
            );
        } catch (IndexMissingException e) {
            throw new IndexNotExistsException("Index[" + indexName + "] is not exists.");
        } catch (TypeMissingException e) {
            throw new TypeNotExistsException("Type[" + typeIdentifier.toString() + "] is not exists.");
        }
        return typeDefinitionList.get(0);
    }

    final boolean exists(final TypeDefinition typeDefinition) throws SQL4ESException {
        final class InternalIndicesRequestBuilder$TypeExists implements
                InternalIndicesRequestBuilder<TypesExistsRequest, TypesExistsResponse,
                        TypesExistsRequestBuilder, TypesExistsAction, TypeDefinition> {

            @Override
            public final TypesExistsAction buildAction() {
                return TypesExistsAction.INSTANCE;
            }

            @Override
            public final TypesExistsRequest buildRequest(final IndicesAdminClient adminClient,
                                                         final TypeDefinition typeDefinition) throws SQL4ESException {
                final TypesExistsRequestBuilder builder = new TypesExistsRequestBuilder(
                        adminClient,
                        typeDefinition.getIndexDefinition().getIdentifier().toString()
                );
                builder.setTypes(typeDefinition.getIdentifier().toString());
                return builder.request();
            }
        }
        final class Result {
            private boolean exists;
        }
        final Result result = new Result();
        this.advancedClient.execute(
                new InternalIndicesRequestBuilder$TypeExists(),
                new AdvancedClient.ResponseCallback<TypesExistsResponse>() {
                    @Override
                    public void callback(final TypesExistsResponse response) throws SQL4ESException {
                        result.exists = response.isExists();
                    }
                },
                typeDefinition
        );
        return result.exists;
    }

}
