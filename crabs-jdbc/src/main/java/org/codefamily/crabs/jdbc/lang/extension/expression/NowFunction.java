package org.codefamily.crabs.jdbc.lang.extension.expression;

import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.exception.CrabsException;
import org.codefamily.crabs.jdbc.lang.expression.Function;
import org.codefamily.crabs.jdbc.lang.expression.NonAggregation;

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
    public final DataType getResultType() throws CrabsException {
        return DataType.DATE;
    }
}
