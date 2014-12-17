package org.codefamily.crabs.jdbc.lang.expression;

import org.codefamily.crabs.util.ReadonlyList;
import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.exception.CrabsException;
import org.codefamily.crabs.jdbc.lang.Expression;

public final class Null extends Expression {

    public static final Null INSTANCE = new Null();

    private Null() {
        // nothing to do.
    }

    @Override
    public final int hashCode() {
        return 0;
    }

    @Override
    public final boolean equals(final Object object) {
        return object == this;
    }

    @Override
    protected final ReadonlyList<Expression> doGetOperandExpressionList() {
        return EMPTY_EXPRESSION_LIST;
    }

    @Override
    protected final String doToString() {
        return "NULL";
    }

    @Override
    public DataType getResultType() throws CrabsException {
        return null;
    }

}
