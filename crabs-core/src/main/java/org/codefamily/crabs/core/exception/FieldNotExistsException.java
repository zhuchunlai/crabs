package org.codefamily.crabs.core.exception;

import org.codefamily.crabs.core.TypeDefinition;

/**
 * Field在指定的Type中不存在时抛出此异常
 *
 * @author zhuchunlai
 * @version $Id: FieldNotExistException.java, v1.0 2014/07/30 16:24 $
 */
public final class FieldNotExistsException extends RuntimeException {

    public FieldNotExistsException(final TypeDefinition.FieldDefinition fieldDefinition) {
        this("Field[" + fieldDefinition.getIdentifier() + "] is not exists.");
    }

    public FieldNotExistsException(final String message) {
        super(message);
    }

}
