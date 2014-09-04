package com.code.crabs.core.exception;

import com.code.crabs.core.TypeDefinition;

/**
 * @author zhuchunlai
 * @version $Id: PrimaryFieldAlreadyExistsException.java, v1.0 2014/08/14 16:14 $
 */
public final class PrimaryFieldAlreadyExistsException extends RuntimeException {

    public PrimaryFieldAlreadyExistsException(final TypeDefinition typeDefinition) {
        super("There's already exists primary field[" +
                typeDefinition.getPrimaryFieldDefinition().getIdentifier().toString() +
                "] in the Type[" +
                typeDefinition.getIndexDefinition().getIdentifier().toString() +
                "." + typeDefinition.getIdentifier().toString() +
                "]");
    }

}
