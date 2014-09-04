package org.codefamily.crabs.core.exception;

import org.codefamily.crabs.core.IndexDefinition;

/**
 * @author zhuchunlai
 * @version $Id: IndexAlreadyExistsException.java, v1.0 2014/08/04 13:46 $
 */
public final class IndexAlreadyExistsException extends RuntimeException {

    public IndexAlreadyExistsException(final IndexDefinition indexDefinition) {
        this("Index[" + indexDefinition.getIdentifier().toString() + "] is already exists.");
    }

    public IndexAlreadyExistsException(final String message) {
        super(message);
    }

}
