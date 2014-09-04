package org.codefamily.crabs.common.util;

/**
 * 字符处理工具
 *
 * @author zhuchunlai
 * @version $Id: StringUtils.java, v1.0 2014/07/30 17:07 $
 */
public final class StringUtils {

    private StringUtils() {
        // nothing to do.
    }

    /**
     * 判断给定字符串是否是<code>null</code>或者<code>""</code>
     *
     * @param value 需要判断的字符串
     * @return <code>true</code>，字符串是<code>null</code>或者<code>""</code>；反之则是<code>false</code>
     */
    public static boolean isNullOrEmptyAfterTrim(final String value) {
        return value == null || value.trim().length() == 0;
    }

}
