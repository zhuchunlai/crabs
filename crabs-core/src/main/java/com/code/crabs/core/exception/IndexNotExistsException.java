package com.code.crabs.core.exception;

import com.code.crabs.core.IndexDefinition;

/**
 * @author zhuchunlai
 * @version $Id: IndexNotExistsException.java, v1.0 2014/08/04 13:47 $
 */
public final class IndexNotExistsException extends RuntimeException {

    public IndexNotExistsException(final IndexDefinition indexDefinition) {
        this("Index[" + indexDefinition.getIdentifier().toString() + "] is not exists.");
    }


    public IndexNotExistsException(final String message) {
        super(message);
    }

}
