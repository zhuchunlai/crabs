package com.code.crabs.exception;

public final class crabsError extends Error {

    private static final long serialVersionUID = 1811695293470214630L;

    public crabsError() {
        // to do nothing.
    }

    public crabsError(final String message) {
        this(message, null);
    }

    public crabsError(final Throwable cause) {
        this(null, cause);
    }

    public crabsError(final String message, final Throwable cause) {
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
