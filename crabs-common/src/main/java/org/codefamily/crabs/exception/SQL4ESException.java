package org.codefamily.crabs.exception;

/**
 * General exception for crabs.
 *
 * @author zhuchunlai
 * @version $Id: SQL4ESException.java, v1.0 2014/08/01 17:53$
 */
public class SQL4ESException extends Exception {

    public SQL4ESException(final String message){
        super(message);
    }

    public SQL4ESException(final Exception e){
        super(e.getMessage(), e);
    }

    public SQL4ESException(final String message, final Throwable cause){
        super(message, cause);
    }


}
