package com.code.crabs.jdbc.lang.extension.expression;

import com.code.crabs.core.DataType;
import com.code.crabs.exception.crabsException;
import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.expression.NonAggregation;

public final class NotExpression extends NonAggregation {

    public NotExpression(final Expression expression) {
        super(expression);
    }

    @Override
    protected final String doToString() {
        return "NOT " + this.getOperandExpression(0).toString();
    }

    @Override
    public final DataType getResultType() throws crabsException {
        return DataType.BOOLEAN;
    }
}
