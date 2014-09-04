package com.code.crabs.jdbc.lang.extension.expression;

import com.code.crabs.core.DataType;
import com.code.crabs.exception.SQL4ESException;
import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.expression.Aggregation;
import com.code.crabs.jdbc.lang.expression.Function;

public final class MininumFunction extends Aggregation implements Function {

    public static final String IDENTIFIER = "MIN";

    public MininumFunction(final Expression expression) throws SQL4ESException {
        super(expression);
    }

    @Override
    public final String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected final String doToString() {
        return IDENTIFIER + "(" + this.getOperandExpression(0).toString() + ")";
    }

    @Override
    public final DataType getResultType() throws SQL4ESException {
        return DataType.DOUBLE;
    }

}
