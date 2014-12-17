package org.codefamily.crabs;

/**
 * 常量
 *
 * @author zhuchunlai
 * @version $Id: Constants.java, v1.0 2014/07/30 17:07 $
 */
public interface Constants {

    static final String FIELD_INDEX_SHARDS_NUM = "number_of_shards";

    static final String FIELD_INDEX_REPLICAS_NUM = "number_of_replicas";

    /**
     * Index默认的shard数量
     */
    static final int DEFAULT_INDEX_SHARDS_NUM = 5;

    /**
     * Index默认的replicas数量
     */
    static final int DEFAULT_INDEX_REPLICAS_NUM = 1;


    /**
     * =============================================
     * 以下是定义的各类日期格式
     * =============================================
     */

    static final String PATTERN_YYYY_MM_DD = "yyyy-MM-dd";

    static final String PATTERN_YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    static final String PATTERN_YYYY_MM_DD_HH_MM_SS_SSS = "yyyy-MM-dd HH:mm:ss.SSS";

    static final String WHITESPACE = " ";

    static final String EMPTY_STRING = "";

    static final byte BOOLEAN_TRUE_BYTE = 1;

    static final byte BOOLEAN_FALSE_BYTE = 0;
}
