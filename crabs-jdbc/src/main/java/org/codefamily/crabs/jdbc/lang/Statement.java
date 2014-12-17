package org.codefamily.crabs.jdbc.lang;

import org.codefamily.crabs.util.ReadonlyList;
import org.codefamily.crabs.exception.CrabsException;

public abstract class Statement {

    protected Statement() {
        // nothing to do.
    }

    private Integer parameterCount;

    private ReadonlyList<Expression> topLevelExpressionList;

    private Integer hashCode;

    private String stringValue;

    @Override
    public final int hashCode() {
        if (this.hashCode == null) {
            this.hashCode = this.toString().hashCode();
        }
        return this.hashCode;
    }

    @Override
    public final String toString() {
        if (this.stringValue == null) {
            try {
                this.stringValue = this.doToString();
            } catch (CrabsException exception) {
                throw new RuntimeException(exception.getMessage(), exception);
            }
        }
        return this.stringValue;
    }

    public final int getParameterCount() throws CrabsException {
        if (this.parameterCount == null) {
            this.parameterCount = this.doGetParameterCount();
        }
        return this.parameterCount;
    }

    public final ReadonlyList<Expression> getTopLevelExpressionList() throws CrabsException {
        if (this.topLevelExpressionList == null) {
            this.topLevelExpressionList = this.doGetTopLevelExpressionList();
        }
        return this.topLevelExpressionList;
    }

    @Override
    public abstract boolean equals(Object object);

    protected abstract String doToString() throws CrabsException;

    protected abstract int doGetParameterCount() throws CrabsException;

    protected abstract ReadonlyList<Expression> doGetTopLevelExpressionList() throws CrabsException;

}
