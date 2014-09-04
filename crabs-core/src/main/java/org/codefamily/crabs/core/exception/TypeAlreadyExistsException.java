package org.codefamily.crabs.core.exception;

import org.codefamily.crabs.core.TypeDefinition;

/**
 * @author zhuchunlai
 * @version $Id: TypeAlreadyExistsException.java, v1.0 2014/08/04 13:44 $
 */
public final class TypeAlreadyExistsException extends RuntimeException {

    public TypeAlreadyExistsException(final TypeDefinition typeDefinition) {
        this("Type[" + typeDefinition.getIdentifier().toString() + "] is already exists.");
    }

    public TypeAlreadyExistsException(final String message) {
        super(message);
    }

}
