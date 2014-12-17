package org.codefamily.crabs.core;

import org.codefamily.crabs.util.StringUtils;

/**
 * 标识类，通常用于表示特定类型实例的名称标识，如：TypeDefinition的名称标识
 *
 * @author zhuchunlai
 * @version $Id: Identifier.java, v1.0 2014/07/30 15:36 $
 */
public final class Identifier {

    private final String value;

    private final int hashCode;

    public Identifier(final String value) {
        if (StringUtils.isNullOrEmptyAfterTrim(value)) {
            throw new IllegalArgumentException("Value is required.");
        }
        this.value = value.intern();
        this.hashCode = this.value.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof Identifier
                && this.value == ((Identifier) obj).value;
    }

    @Override
    public final String toString() {
        return this.value;
    }

    @Override
    public final int hashCode() {
        return this.hashCode;
    }

}
