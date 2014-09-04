package org.codefamily.crabs.jdbc.lang.expression;

import org.codefamily.crabs.common.util.ReadonlyList;
import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.context.Context;
import org.codefamily.crabs.exception.SQL4ESException;

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
