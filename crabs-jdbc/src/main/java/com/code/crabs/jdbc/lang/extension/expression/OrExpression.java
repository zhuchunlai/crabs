package com.code.crabs.jdbc.lang.extension.expression;

import com.code.crabs.core.DataType;
import com.code.crabs.exception.crabsException;
import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.expression.NonAggregation;

public final class OrExpression extends NonAggregation {

    public OrExpression(final Expression expression1,
                        final Expression expression2) {
        super(expression1, expression2);
    }

    @Override
    protected final String doToString() {
        return this.getOperandExpression(0).toString() + " OR "
                + this.getOperandExpression(1).toString();
    }

    @Override
    public final DataType getResultType() throws crabsException {
        return DataType.BOOLEAN;
    }
}
