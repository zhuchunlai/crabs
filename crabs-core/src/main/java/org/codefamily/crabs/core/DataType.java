package org.codefamily.crabs.core;

import org.codefamily.crabs.core.exception.UnsupportedDataTypeException;
import org.codefamily.crabs.exception.CrabsException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * 当前支持的数据类型定义
 *
 * @author zhuchunlai
 * @version $Id: DataType.java, v1.0 2014/07/30 15:36 $
 */
public enum DataType {

    STRING {
        @Override
        public final Class getJavaType() {
            return String.class;
        }

        @Override
        public final String getElasticsearchType() {
            return "string";
        }

        @Override
        @SuppressWarnings("unchecked")
        public final String toValue(final String value, final String pattern) {
            return value;
        }

        @Override
        public final int displaySize() {
            return 40;
        }

        @Override
        public final int getValueSize() {
            // todo 完善
            return Long.SIZE;
        }
    },

    LONG {
        @Override
        public final Class getJavaType() {
            return Long.class;
        }

        @Override
        public final String getElasticsearchType() {
            return "long";
        }

        @Override
        @SuppressWarnings("unchecked")
        public final Long toValue(final String value, final String pattern) {
            return Long.valueOf(value);
        }

        @Override
        public final int displaySize() {
            return 40;
        }

        @Override
        public final int getValueSize() {
            return Long.SIZE;
        }
    },

    INTEGER {
        @Override
        public final Class getJavaType() {
            return Integer.class;
        }

        @Override
        public final String getElasticsearchType() {
            return "integer";
        }

        @Override
        @SuppressWarnings("unchecked")
        public final Integer toValue(final String value, final String pattern) {
            return Integer.parseInt(value);
        }

        @Override
        public final int displaySize() {
            return 20;
        }

        @Override
        public final int getValueSize() {
            return Integer.SIZE;
        }

    },

    FLOAT {
        @Override
        public final Class getJavaType() {
            return Float.class;
        }

        @Override
        public final String getElasticsearchType() {
            return "float";
        }

        @Override
        @SuppressWarnings("unchecked")
        public Float toValue(final String value, final String pattern) {
            return Float.valueOf(value);
        }

        @Override
        public final int displaySize() {
            return 20;
        }

        @Override
        public final int getValueSize() {
            return Float.SIZE;
        }
    },

    DOUBLE {
        @Override
        public final Class getJavaType() {
            return Double.class;
        }

        @Override
        public final String getElasticsearchType() {
            return "double";
        }

        @Override
        @SuppressWarnings("unchecked")
        public final Double toValue(final String value, final String pattern) {
            return Double.parseDouble(value);
        }

        @Override
        public final int displaySize() {
            return 40;
        }

        @Override
        public final int getValueSize() {
            return Double.SIZE;
        }
    },

    BOOLEAN {
        @Override
        public final Class getJavaType() {
            return Boolean.class;
        }

        @Override
        public final String getElasticsearchType() {
            return "boolean";
        }

        @Override
        @SuppressWarnings("unchecked")
        public final Boolean toValue(final String value, final String pattern) {
            return Boolean.parseBoolean(value);
        }

        @Override
        public final int displaySize() {
            return 10;
        }

        @Override
        public final int getValueSize() {
            return Byte.SIZE;
        }
    },

    DATE {
        @Override
        public final Class getJavaType() {
            return Date.class;
        }

        @Override
        public final String getElasticsearchType() {
            return "date";
        }

        @Override
        @SuppressWarnings("unchecked")
        public final Date toValue(final String value, final String pattern) throws CrabsException {
            final SimpleDateFormat format = new SimpleDateFormat(pattern);
            try {
                return format.parse(value);
            } catch (ParseException e) {
                throw new CrabsException(e.getMessage(), e);
            }
        }

        @Override
        public final int displaySize() {
            return 50;
        }

        @Override
        public final int getValueSize() {
            return 18;
        }
    };

    public abstract Class getJavaType();

    public abstract String getElasticsearchType();

    public abstract <T> T toValue(final String value, final String pattern) throws CrabsException;

    public abstract int displaySize();

    public abstract int getValueSize();

    public static DataType getDataTypeWithElasticsearchType(final String elasticsearchType) {
        final DataType dataType = DATA_TYPE_ELASTICSEARCH_TYPE_MAP.get(elasticsearchType);
        if (dataType == null) {
            throw new UnsupportedDataTypeException("Unsupported elasticsearch type[" + elasticsearchType + "]");
        }
        return dataType;
    }

    public static DataType getDataType(final Class<?> valueJavaClass) {
        final DataType dataType = VALUE_CLASS_DATA_TYPE_MAP.get(valueJavaClass);
        if (dataType == null) {
            throw new UnsupportedDataTypeException("Class[" + valueJavaClass.getName() + "] is not supported.");
        }
        return dataType;
    }

    private static final HashMap<String, DataType> DATA_TYPE_ELASTICSEARCH_TYPE_MAP = new HashMap<String, DataType>();

    private static final HashMap<Class, DataType> VALUE_CLASS_DATA_TYPE_MAP = new HashMap<Class, DataType>();

    static {
        for (DataType dataType : values()) {
            DATA_TYPE_ELASTICSEARCH_TYPE_MAP.put(dataType.getElasticsearchType(), dataType);
            VALUE_CLASS_DATA_TYPE_MAP.put(dataType.getJavaType(), dataType);
        }
    }

}
