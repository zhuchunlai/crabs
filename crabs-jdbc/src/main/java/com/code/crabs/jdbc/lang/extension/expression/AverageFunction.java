package com.code.crabs.jdbc.lang.extension.expression;

import com.code.crabs.core.DataType;
import com.code.crabs.exception.crabsException;
import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.expression.Aggregation;
import com.code.crabs.jdbc.lang.expression.Function;

/**
 * @author zhuchunlai
 * @version $Id: AverageFunction.java, v1.0 2014/08/26 16:27 $
 */
public final class AverageFunction extends Aggregation implements Function {

    public static final String IDENTIFIER = "AVG";

    public AverageFunction(final Expression expression) throws crabsException {
        super(expression);
    }

    @Override
    public final DataType getResultType() throws crabsException {
        return DataType.DOUBLE;
    }

    @Override
    protected String doToString() throws crabsException {
        return IDENTIFIER + "(" + this.getOperandExpression(0).toString() + ")";
    }

    @Override
    public final String getIdentifier() {
        return IDENTIFIER;
    }
}
