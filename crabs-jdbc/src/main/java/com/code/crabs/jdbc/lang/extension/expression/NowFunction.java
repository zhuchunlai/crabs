package com.code.crabs.jdbc.lang.extension.expression;

import com.code.crabs.core.DataType;
import com.code.crabs.exception.crabsException;
import com.code.crabs.jdbc.lang.expression.Function;
import com.code.crabs.jdbc.lang.expression.NonAggregation;

public final class NowFunction extends NonAggregation implements Function {

    public static final String IDENTIFIER = "NOW";

    public NowFunction() {
        // to do nothing.
    }

    @Override
    public final String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected final String doToString() {
        return IDENTIFIER + "()";
    }

    @Override
    public final DataType getResultType() throws crabsException {
        return DataType.DATE;
    }
}
