package com.code.crabs.jdbc.lang.extension.expression;

import com.code.crabs.core.DataType;
import com.code.crabs.exception.SQL4ESException;
import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.expression.Function;
import com.code.crabs.jdbc.lang.expression.NonAggregation;

public final class SubstringFunction extends NonAggregation implements Function {

	public static final String IDENTIFIER = "SUBSTRING";

	public SubstringFunction(final Expression expression1,
			final Expression expression2, final Expression expression3) {
		super(expression1, expression2, expression3);
	}

	@Override
	public final String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	protected final String doToString() {
		return IDENTIFIER + "(" + this.getOperandExpression(0) + ", "
				+ this.getOperandExpression(1).toString() + ", "
				+ this.getOperandExpression(2).toString() + ")";
	}

    @Override
    public final DataType getResultType() throws SQL4ESException {
        return DataType.STRING;
    }
}
