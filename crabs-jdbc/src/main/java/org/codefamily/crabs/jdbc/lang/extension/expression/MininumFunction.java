package org.codefamily.crabs.jdbc.lang.extension.expression;

import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.exception.CrabsException;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.Aggregation;
import org.codefamily.crabs.jdbc.lang.expression.Function;

public final class MininumFunction extends Aggregation implements Function {

    public static final String IDENTIFIER = "MIN";

    public MininumFunction(final Expression expression) throws CrabsException {
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
    public final DataType getResultType() throws CrabsException {
        return DataType.DOUBLE;
    }

}
