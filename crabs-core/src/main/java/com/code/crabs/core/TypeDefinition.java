package com.code.crabs.core;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.common.util.StringUtils;
import com.code.crabs.core.exception.FieldAlreadyExistsException;
import com.code.crabs.core.exception.FieldNotExistsException;
import com.code.crabs.core.exception.PrimaryFieldAlreadyExistsException;
import com.code.crabs.core.exception.PrimaryFieldNotFoundException;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 定义elasticsearch的type的逻辑结构
 *
 * @author zhuchunlai
 * @version $Id: TypeDefinition.java, v1.0 2014/07/30 15:36 $
 */
public final class TypeDefinition {

    private final IndexDefinition indexDefinition;

    private final Identifier identifier;

    private final boolean ttlEnabled;

    private final boolean allEnabled;

    private final HashMap<Identifier, FieldDefinition> fieldMap;

    private final ArrayList<FieldDefinition> fieldList;

    // 主键列
    private FieldDefinition primaryFieldDefinition;

    private boolean published;

    /**
     * 构造方法，默认关闭TTL和全文检索功能
     *
     * @param indexDefinition type所属的index
     * @param identifier      type名称标识
     */
    public TypeDefinition(final IndexDefinition indexDefinition,
                          final Identifier identifier) {
        this(indexDefinition, identifier, false);
    }

    /**
     * 构造方法，默认关闭TTL
     *
     * @param indexDefinition type所属的index
     * @param identifier      type名称标识
     * @param allEnabled      是否开启全文检索功能，<code>true</code>，表示开启；反之关闭
     */
    public TypeDefinition(final IndexDefinition indexDefinition,
                          final Identifier identifier,
                          final boolean allEnabled) {
        this(indexDefinition, identifier, false, allEnabled);
    }

    /**
     * 构造方法
     *
     * @param indexDefinition type所属的index
     * @param identifier      type名称标识
     * @param ttlEnabled      是否开启TTL功能，<code>true</code>，表示开启；反之关闭
     * @param allEnabled      是否开启全文检索功能，<code>true</code>，表示开启；反之关闭
     */
    public TypeDefinition(final IndexDefinition indexDefinition,
                          final Identifier identifier,
                          final boolean ttlEnabled,
                          final boolean allEnabled) {
        if (indexDefinition == null) {
            throw new IllegalArgumentException("Argument[indexDefinition] is required.");
        }
        if (identifier == null) {
            throw new IllegalArgumentException("Argument[identifier] is required.");
        }
        this.indexDefinition = indexDefinition;
        this.identifier = identifier;
        this.ttlEnabled = ttlEnabled;
        this.allEnabled = allEnabled;
        this.fieldMap = new HashMap<Identifier, FieldDefinition>();
        this.fieldList = new ArrayList<FieldDefinition>();
        this.published = false;
    }

    public final FieldDefinition defineStringField(final Identifier identifier) {
        return this.defineField(identifier, DataType.STRING, null, false);
    }

    public final FieldDefinition defineStringField(final Identifier identifier, final boolean stored) {
        return this.defineField(identifier, DataType.STRING, null, stored);
    }

    public final FieldDefinition defineLongField(final Identifier identifier) {
        return this.defineField(identifier, DataType.LONG, null, false);
    }

    public final FieldDefinition defineLongField(final Identifier identifier, final boolean stored) {
        return this.defineField(identifier, DataType.LONG, null, stored);
    }

    public final FieldDefinition defineIntegerField(final Identifier identifier) {
        return this.defineField(identifier, DataType.INTEGER, null, false);
    }

    public final FieldDefinition defineIntegerField(final Identifier identifier, final boolean stored) {
        return this.defineField(identifier, DataType.INTEGER, null, stored);
    }

    public final FieldDefinition defineFloatField(final Identifier identifier) {
        return this.defineField(identifier, DataType.FLOAT, null, false);
    }

    public final FieldDefinition defineFloatField(final Identifier identifier, final boolean stored) {
        return this.defineField(identifier, DataType.FLOAT, null, stored);
    }

    public final FieldDefinition defineDoubleField(final Identifier identifier) {
        return this.defineField(identifier, DataType.DOUBLE, null, false);
    }

    public final FieldDefinition defineDoubleField(final Identifier identifier, final boolean stored) {
        return this.defineField(identifier, DataType.DOUBLE, null, stored);
    }

    public final FieldDefinition defineBooleanField(final Identifier identifier) {
        return this.defineField(identifier, DataType.BOOLEAN, null, false);
    }

    public final FieldDefinition defineBooleanField(final Identifier identifier, final boolean stored) {
        return this.defineField(identifier, DataType.BOOLEAN, null, stored);
    }

    public final FieldDefinition defineDateField(final Identifier identifier, final String pattern) {
        return this.defineField(identifier, DataType.DATE, pattern, false);
    }

    public final FieldDefinition defineDateField(final Identifier identifier,
                                                 final String pattern,
                                                 final boolean stored) {
        return this.defineField(identifier, DataType.DATE, pattern, stored);
    }

    public final boolean isPublished() {
        return this.published;
    }

    private int fieldDefinitionCount;

    private ReadonlyList<FieldDefinition> fieldDefinitionReadonlyList;

    public final void publish() {
        if (!this.published) {
            synchronized (this) {
                if (!this.published) {
                    if (this.primaryFieldDefinition == null) {
                        throw new PrimaryFieldNotFoundException(this);
                    }
                    this.fieldDefinitionReadonlyList = ReadonlyList.newInstance(this.fieldList);
                    this.fieldDefinitionCount = this.fieldDefinitionReadonlyList.size();
                    this.published = true;
                }
            }
        }
    }

    public final int getFieldDefinitionCount() {
        if (!this.published) {
            throw new IllegalStateException("TypeDefinition is not published. Please invoke publish() at first.");
        }
        return this.fieldDefinitionCount;
    }

    public final FieldDefinition getFieldDefinition(final Identifier identifier) {
        if (identifier == null) {
            throw new IllegalArgumentException("Argument[identifier] is required.");
        }
        if (!this.published) {
            throw new IllegalStateException("TypeDefinition is not published. Please invoke publish() at first.");
        }
        final FieldDefinition fieldDefinition = this.fieldMap.get(identifier);
        if (fieldDefinition == null) {
            throw new FieldNotExistsException("Field[" + identifier + "] does not exist in the type[" + this.identifier + "].");
        }
        return fieldDefinition;
    }

    public final FieldDefinition getFieldDefinition(final int fieldIndex) {
        final int count = this.getFieldDefinitionCount();
        if (fieldIndex < 0 || fieldIndex >= count) {
            throw new IndexOutOfBoundsException("fieldIndex must between 0 and " + count);
        }
        if (!this.published) {
            throw new IllegalStateException("TypeDefinition is not published. Please invoke publish() at first.");
        }
        return this.fieldDefinitionReadonlyList.get(fieldIndex);
    }

    public final boolean containsField(final Identifier identifier) {
        if (!this.published) {
            throw new IllegalStateException("TypeDefinition is not published. Please invoke publish() at first.");
        }
        return this.fieldMap.containsKey(identifier);
    }

    public final ReadonlyList<FieldDefinition> getAllFieldDefinitions() {
        if (!this.published) {
            throw new IllegalStateException("TypeDefinition is not published. Please invoke publish() at first.");
        }
        return this.fieldDefinitionReadonlyList;
    }

    public final FieldDefinition getPrimaryFieldDefinition() {
        if (!this.published) {
            throw new IllegalStateException("TypeDefinition is not published. Please invoke publish() at first.");
        }
        return this.primaryFieldDefinition;
    }

    public final Identifier getIdentifier() {
        return this.identifier;
    }

    public final IndexDefinition getIndexDefinition() {
        return this.indexDefinition;
    }

    public final boolean isTTLEnabled() {
        return this.ttlEnabled;
    }

    public final boolean isAllEnabled() {
        return this.allEnabled;
    }

    @Override
    public final boolean equals(final Object obj) {
        return obj instanceof TypeDefinition
                && this.identifier.equals(((TypeDefinition) obj).identifier);
    }

    private int nextFieldDefinitionIndex = 0;

    public final FieldDefinition defineField(final Identifier identifier,
                                             final DataType dataType,
                                             final String pattern,
                                             final boolean stored) {
        if (identifier == null) {
            throw new IllegalArgumentException("Argument[identifier] is required.");
        }
        if (dataType == null) {
            throw new IllegalArgumentException("Argument[dataType] is required.");
        }
        if (dataType == DataType.DATE && StringUtils.isNullOrEmptyAfterTrim(pattern)) {
            throw new IllegalArgumentException("Argument[pattern] is required for data type[" + DataType.DATE + "]");
        }
        if (this.fieldMap.containsKey(identifier)) {
            throw new FieldAlreadyExistsException("Field[" + identifier + "] is already exist.");
        }
        final int fieldDefinitionIndex;
        synchronized (this) {
            fieldDefinitionIndex = this.nextFieldDefinitionIndex++;
        }
        final FieldDefinition fieldDefinition
                = new FieldDefinition(identifier, dataType, pattern, stored, fieldDefinitionIndex);
        this.fieldMap.put(identifier, fieldDefinition);
        this.fieldList.add(fieldDefinition);
        return fieldDefinition;
    }

    /**
     * 定义elasticsearch中的Field结构
     */
    public final class FieldDefinition {

        private final Identifier identifier;

        private final DataType dataType;

        // pattern信息，仅对DATE类型的字段有效
        private final String pattern;

        private final boolean stored;

        boolean isPrimaryField;

        final int fieldDefinitionIndex;

        /**
         * 构造方法
         *
         * @param identifier field名称标识
         * @param dataType   field的数据类型
         * @param pattern    pattern信息，用于格式化DATE类型的数据
         * @param stored     field是否单独存储，对应与elasticsearch中的概念，<code>true</code>，表示单独存储
         */
        FieldDefinition(final Identifier identifier,
                        final DataType dataType,
                        final String pattern,
                        final boolean stored,
                        final int fieldDefinitionIndex) {
            this.identifier = identifier;
            this.dataType = dataType;
            this.pattern = pattern;
            this.stored = stored;
            this.fieldDefinitionIndex = fieldDefinitionIndex;
            this.isPrimaryField = false;
        }

        public final Identifier getIdentifier() {
            return this.identifier;
        }

        public final DataType getDataType() {
            return this.dataType;
        }

        public final String getPattern() {
            return this.pattern;
        }

        public final boolean isStored() {
            return this.stored;
        }

        public final boolean isPrimaryField() {
            return this.isPrimaryField;
        }

        public final TypeDefinition getTypeDefinition() {
            return TypeDefinition.this;
        }

        public final void asPrimaryField() {
            if (TypeDefinition.this.primaryFieldDefinition != null) {
                throw new PrimaryFieldAlreadyExistsException(TypeDefinition.this);
            }
            TypeDefinition.this.primaryFieldDefinition = this;
            this.isPrimaryField = true;
        }

        @Override
        public final boolean equals(final Object obj) {
            return obj instanceof FieldDefinition
                    && this.identifier.equals(((FieldDefinition) obj).identifier);
        }

        @Override
        public final int hashCode() {
            return this.identifier.hashCode();
        }
    }

}
