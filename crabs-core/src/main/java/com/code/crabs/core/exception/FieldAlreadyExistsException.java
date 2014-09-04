package com.code.crabs.core.exception;

import com.code.crabs.core.TypeDefinition;

/**
 * Field在指定的Type中已经存在时抛出此异常
 *
 * @author zhuchunlai
 * @version $Id: FieldAlreadyExistException.java, v1.0 2014/07/30 16:24 $
 */
public final class FieldAlreadyExistsException extends RuntimeException {

    public FieldAlreadyExistsException(final TypeDefinition.FieldDefinition fieldDefinition) {
        this("Field[" + fieldDefinition.getIdentifier() + "] is already exists.");
    }

    public FieldAlreadyExistsException(final String message) {
        super(message);
    }

}
