package org.codefamily.crabs.jdbc.lang.extension.expression;

import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.exception.CrabsException;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.Constant;
import org.codefamily.crabs.jdbc.lang.expression.NonAggregation;
import org.codefamily.crabs.jdbc.lang.extension.ReservedKeyword;

public final class LikeExpression extends NonAggregation {

    public LikeExpression(final Expression expression1,
                          final Expression expression2) {
        super(expression1, expression2);
        if (expression2 instanceof Constant) {
            final Constant constant = Constant.class.cast(expression2);
            if (!(constant.value instanceof String)) {
                throw new IllegalArgumentException("Argument [expression2] must be a string expression.");
            }
        }
    }

    @Override
    protected final String doToString() {
        final Expression expression2 = this.getOperandExpression(1);
        final boolean isExpression2Constant = expression2 instanceof Constant;
        return this.getOperandExpression(0).toString() + " "
                + ReservedKeyword.LIKE + (isExpression2Constant ? " '" : " ")
                + expression2.toString()
                + (isExpression2Constant ? "'" : "");
    }

    @Override
    public final DataType getResultType() throws CrabsException {
        return DataType.BOOLEAN;
    }

}
