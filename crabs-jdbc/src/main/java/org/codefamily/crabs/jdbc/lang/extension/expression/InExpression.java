package org.codefamily.crabs.jdbc.lang.extension.expression;

import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.exception.CrabsException;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.NonAggregation;
import org.codefamily.crabs.jdbc.lang.extension.ReservedKeyword;

public final class InExpression extends NonAggregation {

    public InExpression(final Expression... expressions) {
        super(expressions);
        if (expressions.length < 1) {
            throw new IllegalArgumentException("Expression count must be more than one.");
        }
        this.expressionCountInSet = expressions.length - 1;
    }

    public final int expressionCountInSet;

    @Override
    protected final String doToString() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.getOperandExpression(0).toString());
        stringBuilder.append(' ');
        stringBuilder.append(ReservedKeyword.IN.name());
        stringBuilder.append(' ');
        stringBuilder.append('(');
        for (int i = 1; i <= this.expressionCountInSet; i++) {
            if (i != 1) {
                // 非第一个表达式
                stringBuilder.append(',');
                stringBuilder.append(' ');
            }
            stringBuilder.append(this.getOperandExpression(i).toString());
        }
        stringBuilder.append(')');
        return stringBuilder.toString();
    }

    @Override
    public final DataType getResultType() throws CrabsException {
        return DataType.BOOLEAN;
    }

}
