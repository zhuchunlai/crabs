package org.codefamily.crabs.exception;

/**
 * General exception for crabs.
 *
 * @author zhuchunlai
 * @version $Id: CrabsException.java, v1.0 2014/08/01 17:53$
 */
public class CrabsException extends Exception {

    public CrabsException(final String message){
        super(message);
    }

    public CrabsException(final Exception e){
        super(e.getMessage(), e);
    }

    public CrabsException(final String message, final Throwable cause){
        super(message, cause);
    }


}
