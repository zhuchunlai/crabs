package com.code.crabs.jdbc.lang.expression;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.core.DataType;
import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.expression.context.Context;
import com.code.crabs.exception.SQL4ESException;

public final class Argument extends Expression {

    public Argument(final int index) {
        this.index = index;
    }

    public final int index;

    @Override
    public final int hashCode() {
        return this.index;
    }

    @Override
    public final boolean equals(final Object object) {
        if (object != null && object instanceof Argument) {
            final Argument that = (Argument) object;
            return this.index == that.index;
        }
        return false;
    }

    @Override
    protected final ReadonlyList<Expression> doGetOperandExpressionList() {
        return EMPTY_EXPRESSION_LIST;
    }

    @Override
    protected final String doToString() {
        return "?";
    }

    @Override
    public final DataType getResultType() throws SQL4ESException {
        return null;
    }

    @Override
    public final DataType getResultType(final Context context) throws SQL4ESException {
        final Object value = context.getArgumentValue(this);
        return DataType.getDataType(value.getClass());
    }
}
