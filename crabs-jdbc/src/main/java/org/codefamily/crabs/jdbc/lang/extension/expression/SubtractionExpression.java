package org.codefamily.crabs.jdbc.lang.extension.expression;

import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.exception.CrabsException;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.NonAggregation;

public final class SubtractionExpression extends NonAggregation {

    public SubtractionExpression(final Expression expression1,
                                 final Expression expression2) {
        super(expression1, expression2);
    }

    @Override
    protected final String doToString() {
        return this.getOperandExpression(0).toString() + " - "
                + this.getOperandExpression(1).toString();
    }

    @Override
    public final DataType getResultType() throws CrabsException {
        final DataType dataType1 = this.getOperandExpression(0).getResultType();
        final DataType dataType2 = this.getOperandExpression(1).getResultType();
        if (dataType1 == DataType.DOUBLE || dataType2 == DataType.DOUBLE) {
            return DataType.DOUBLE;
        } else if (dataType1 == DataType.FLOAT || dataType2 == DataType.FLOAT) {
            return DataType.FLOAT;
        } else if (dataType1 == DataType.LONG || dataType2 == DataType.LONG) {
            return DataType.LONG;
        } else if (dataType1 == DataType.INTEGER || dataType2 == DataType.INTEGER) {
            return DataType.INTEGER;
        }
        return null;
    }
}
