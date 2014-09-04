package com.code.crabs.exception;

/**
 * @author zhuchunlai
 * @version $Id: crabsException.java, v1.0 2014/08/04 17:53 $
 */
public class crabsException extends Exception {

    public crabsException(final String message) {
        super(message);
    }

    public crabsException(final Exception e) {
        this(e.getMessage(), e);
    }

    public crabsException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
