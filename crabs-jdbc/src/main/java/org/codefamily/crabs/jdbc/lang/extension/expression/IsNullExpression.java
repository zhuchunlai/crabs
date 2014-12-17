package org.codefamily.crabs.jdbc.lang.extension.expression;

import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.exception.CrabsException;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.NonAggregation;

public final class IsNullExpression extends NonAggregation {

    public IsNullExpression(final Expression expression) {
        super(expression);
    }

    @Override
    protected final String doToString() {
        return this.getOperandExpression(0).toString() + " IS NULL";
    }

    @Override
    public final DataType getResultType() throws CrabsException {
        return DataType.BOOLEAN;
    }


}
