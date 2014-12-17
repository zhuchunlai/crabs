package org.codefamily.crabs.jdbc.lang.extension.expression;

import org.codefamily.crabs.util.ReadonlyList;
import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.exception.CrabsException;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.Function;
import org.codefamily.crabs.jdbc.lang.expression.NonAggregation;

public final class ConcatFunction extends NonAggregation implements Function {

    public static final String IDENTIFIER = "CONCAT";

    private static final ThreadLocal<StringBuilder> FACTORY$STRING_BUILDER = new ThreadLocal<StringBuilder>() {

        @Override
        protected final StringBuilder initialValue() {
            return new StringBuilder();
        }

    };

    private static StringBuilder getEmptyStringBuilder() {
        final StringBuilder stringBuilder = FACTORY$STRING_BUILDER.get();
        stringBuilder.setLength(0);
        return stringBuilder;
    }

    public ConcatFunction(final Expression... expressions) {
        super(expressions);
        if (expressions.length < 1) {
            throw new IllegalArgumentException(
                    "Expression count must be more than one.");
        }
    }

    @Override
    public final String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected final String doToString() throws CrabsException {
        final ReadonlyList<Expression> operandExpressionList = this.getOperandExpressionList();
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(IDENTIFIER);
        stringBuilder.append('(');
        stringBuilder.append(operandExpressionList.get(0).toString());
        for (int i = 1; i <= operandExpressionList.size(); i++) {
            stringBuilder.append(',');
            stringBuilder.append(' ');
            stringBuilder.append(operandExpressionList.get(i).toString());
        }
        stringBuilder.append(')');
        return stringBuilder.toString();
    }

    @Override
    public final DataType getResultType() throws CrabsException {
        return DataType.STRING;
    }
}
