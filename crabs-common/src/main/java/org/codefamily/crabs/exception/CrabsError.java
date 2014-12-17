package org.codefamily.crabs.exception;

public final class CrabsError extends Error {

    private static final long serialVersionUID = 1811695293470214630L;

    public CrabsError() {
        // to do nothing.
    }

    public CrabsError(final String message) {
        this(message, null);
    }

    public CrabsError(final Throwable cause) {
        this(null, cause);
    }

    public CrabsError(final String message, final Throwable cause) {
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
