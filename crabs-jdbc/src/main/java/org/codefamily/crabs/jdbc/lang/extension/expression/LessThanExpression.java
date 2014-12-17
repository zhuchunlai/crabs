package org.codefamily.crabs.jdbc.lang.extension.expression;

import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.exception.CrabsException;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.NonAggregation;

public final class LessThanExpression extends NonAggregation {

    public LessThanExpression(final Expression expression1,
                              final Expression expression2) {
        super(expression1, expression2);
    }

    @Override
    protected final String doToString() {
        return this.getOperandExpression(0).toString() + " < "
                + this.getOperandExpression(1).toString();
    }

    @Override
    public final DataType getResultType() throws CrabsException {
        return DataType.BOOLEAN;
    }

}
