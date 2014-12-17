package org.codefamily.crabs.jdbc.lang.extension.expression;

import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.exception.CrabsException;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.Aggregation;
import org.codefamily.crabs.jdbc.lang.expression.Function;

/**
 * @author zhuchunlai
 * @version $Id: AverageFunction.java, v1.0 2014/08/26 16:27 $
 */
public final class AverageFunction extends Aggregation implements Function {

    public static final String IDENTIFIER = "AVG";

    public AverageFunction(final Expression expression) throws CrabsException {
        super(expression);
    }

    @Override
    public final DataType getResultType() throws CrabsException {
        return DataType.DOUBLE;
    }

    @Override
    protected String doToString() throws CrabsException {
        return IDENTIFIER + "(" + this.getOperandExpression(0).toString() + ")";
    }

    @Override
    public final String getIdentifier() {
        return IDENTIFIER;
    }
}
