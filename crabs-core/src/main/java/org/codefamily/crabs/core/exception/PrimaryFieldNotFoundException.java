package org.codefamily.crabs.core.exception;

import org.codefamily.crabs.core.TypeDefinition;

/**
 * @author zhuchunlai
 * @version $Id: PrimaryFieldNotFoundException.java, v1.0 2014/08/14 16:15 $
 */
public final class PrimaryFieldNotFoundException extends RuntimeException {

    public PrimaryFieldNotFoundException(final TypeDefinition typeDefinition) {
        super("There's no primary field in the Type[" +
                typeDefinition.getIndexDefinition().getIdentifier().toString() +
                "." + typeDefinition.getIdentifier().toString() +
                "].");
    }

}
