package com.code.crabs.exception;

public final class SQL4ESError extends Error {

    private static final long serialVersionUID = 1811695293470214630L;

    public SQL4ESError() {
        // to do nothing.
    }

    public SQL4ESError(final String message) {
        this(message, null);
    }

    public SQL4ESError(final Throwable cause) {
        this(null, cause);
    }

    public SQL4ESError(final String message, final Throwable cause) {
        super(message, cause);
    }

    private String message;

    @Override
    public final String getMessage() {
        if (this.message == null) {
            return super.getMessage();
        } else {
            return this.message;
        }
    }

}
