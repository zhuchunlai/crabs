package com.code.crabs.jdbc.lang.expression;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.jdbc.lang.Expression;

public abstract class NonAggregation extends Expression {

    protected NonAggregation(final Expression... operandExpressions) {
        if (operandExpressions == null) {
            throw new IllegalArgumentException("Argument[operandExpressions] is null.");
        }
        for (int i = 0; i < operandExpressions.length; i++) {
            if (operandExpressions[i] == null) {
                throw new IllegalArgumentException("Argument[expression[" + i + "]] is null.");
            }
        }
        this.operandExpressions = operandExpressions.clone();
    }

    public transient int index = -1;

    final Expression[] operandExpressions;

    public final Expression getOperandExpression(final int index) {
        return this.operandExpressions[index];
    }

    @Override
    protected final ReadonlyList<Expression> doGetOperandExpressionList() {
        return ReadonlyList.newInstance(this.operandExpressions);
    }

}
