package com.code.crabs.jdbc.lang.extension.expression;

import com.code.crabs.core.DataType;
import com.code.crabs.exception.SQL4ESException;
import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.expression.Constant;
import com.code.crabs.jdbc.lang.expression.NonAggregation;
import com.code.crabs.jdbc.lang.extension.ReservedKeyword;

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
    public final DataType getResultType() throws SQL4ESException {
        return DataType.BOOLEAN;
    }

}
