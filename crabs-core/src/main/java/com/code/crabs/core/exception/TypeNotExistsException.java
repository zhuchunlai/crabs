package com.code.crabs.core.exception;

import com.code.crabs.core.TypeDefinition;

/**
 * @author zhuchunlai
 * @version $Id: TypeNotExistsException.java, v1.0 2014/08/04 13:43 $
 */
public final class TypeNotExistsException extends RuntimeException {

    public TypeNotExistsException(final TypeDefinition typeDefinition) {
        this("Type[" + typeDefinition.getIdentifier() + "] is not exists.");
    }

    public TypeNotExistsException(final String message) {
        super(message);
    }

}
