package com.code.crabs.core;

import com.code.crabs.core.exception.UnsupportedDataTypeException;

import java.sql.Timestamp;
import java.util.Date;

/**
 * 表示存储在elasticsearch type中的单个document
 * <b>说明：Document仅用于插入、更新、删除和按_id检索的场景</b>
 *
 * @author zhuchunlai
 * @version $Id: Document.java, v1.0 2014/07/30 17:16 $
 */
public final class Document {

    private final TypeDefinition typeDefinition;

    private final Object[] values;

    Document(final TypeDefinition typeDefinition) {
        this.typeDefinition = typeDefinition;
        this.values = new Object[this.typeDefinition.getFieldDefinitionCount()];
    }

    public final void setValue(final int fieldIndex, final Object value) {
        final TypeDefinition.FieldDefinition fieldDefinition = this.typeDefinition.getFieldDefinition(fieldIndex);
        this.setValue(fieldDefinition, value);
    }

    public final Object getValue(final int fieldIndex) {
        final int count = this.typeDefinition.getFieldDefinitionCount();
        if (fieldIndex < 0 || fieldIndex >= count) {
            throw new IndexOutOfBoundsException("fieldIndex must between 0 and " + count);
        }
        return this.values[fieldIndex];
    }

    public final void setValue(final Identifier identifier, final Object value) {
        final TypeDefinition.FieldDefinition fieldDefinition = this.typeDefinition.getFieldDefinition(identifier);
        this.setValue(fieldDefinition, value);
    }

    public final Object getValue(final Identifier identifier) {
        final TypeDefinition.FieldDefinition fieldDefinition = this.typeDefinition.getFieldDefinition(identifier);
        return this.values[fieldDefinition.fieldDefinitionIndex];
    }

    public final TypeDefinition getTypeDefinition() {
        return this.typeDefinition;
    }

    public final IndexDefinition getIndexDefinition() {
        return this.typeDefinition.getIndexDefinition();
    }

    private void setValue(final TypeDefinition.FieldDefinition fieldDefinition, final Object value) {
        final int fieldIndex = fieldDefinition.fieldDefinitionIndex;
        switch (fieldDefinition.getDataType()) {
            case STRING:
                this.values[fieldIndex] = (String) value;
                break;
            case INTEGER:
                if (!(value instanceof Number)) {
                    throw new ClassCastException("can not cast " + value.getClass().getName() + " to int.");
                }
                this.values[fieldIndex] = ((Number) value).intValue();
                break;
            case LONG:
                if (!(value instanceof Number)) {
                    throw new ClassCastException("can not cast " + value.getClass().getName() + " to long.");
                }
                this.values[fieldIndex] = ((Number) value).longValue();
                break;
            case FLOAT:
                if (!(value instanceof Number)) {
                    throw new ClassCastException("can not cast " + value.getClass().getName() + " to float.");
                }
                this.values[fieldIndex] = ((Number) value).floatValue();
                break;
            case DOUBLE:
                if (!(value instanceof Number)) {
                    throw new ClassCastException("can not cast " + value.getClass().getName() + " to double.");
                }
                this.values[fieldIndex] = ((Number) value).doubleValue();
                break;
            case BOOLEAN:
                if (value instanceof String) {
                    this.values[fieldIndex] = Boolean.parseBoolean((String) value);
                } else if (value instanceof Boolean) {
                    this.values[fieldIndex] = value;
                } else {
                    throw new ClassCastException("can not cast " + value.getClass().getName() + " to boolean.");
                }
                break;
            case DATE:
                if (!(value instanceof Timestamp || value instanceof Date || value instanceof String)) {
                    throw new ClassCastException("can not cast " + value + " to java.util.Date.");
                }
                if (value instanceof Timestamp) {
                    this.values[fieldIndex] = value;
                } else if (value instanceof Date) {
                    this.values[fieldIndex] = new Timestamp(((Date) value).getTime());
                } else {
                    final String timestampString = (String) value;
                    try {
                        this.values[fieldIndex] = new Timestamp(
                                java.sql.Date.valueOf(timestampString).getTime()
                        );
                    } catch (IllegalArgumentException e) {
                        this.values[fieldIndex] = Timestamp.valueOf(timestampString);
                    }
                }
                break;
            default:
                throw new UnsupportedDataTypeException("DataType[" + fieldDefinition.getDataType() + "] is not supported.");
        }
    }

}
