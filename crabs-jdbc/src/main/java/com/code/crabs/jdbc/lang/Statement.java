package com.code.crabs.jdbc.lang;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.exception.SQL4ESException;

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
            } catch (SQL4ESException exception) {
                throw new RuntimeException(exception.getMessage(), exception);
            }
        }
        return this.stringValue;
    }

    public final int getParameterCount() throws SQL4ESException {
        if (this.parameterCount == null) {
            this.parameterCount = this.doGetParameterCount();
        }
        return this.parameterCount;
    }

    public final ReadonlyList<Expression> getTopLevelExpressionList() throws SQL4ESException {
        if (this.topLevelExpressionList == null) {
            this.topLevelExpressionList = this.doGetTopLevelExpressionList();
        }
        return this.topLevelExpressionList;
    }

    @Override
    public abstract boolean equals(Object object);

    protected abstract String doToString() throws SQL4ESException;

    protected abstract int doGetParameterCount() throws SQL4ESException;

    protected abstract ReadonlyList<Expression> doGetTopLevelExpressionList() throws SQL4ESException;

}
