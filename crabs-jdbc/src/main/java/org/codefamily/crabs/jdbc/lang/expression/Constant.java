package org.codefamily.crabs.jdbc.lang.expression;

import org.codefamily.crabs.common.util.ReadonlyList;
import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.exception.SQL4ESException;
import org.codefamily.crabs.jdbc.lang.Expression;

public final class Constant extends Expression {

    public Constant(final Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Argument[value] is null.");
        }
        this.value = value;
        this.dataType = DataType.getDataType(value.getClass());
    }

    public final Object value;

    public final DataType dataType;

    @Override
    public final int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public final boolean equals(final Object object) {
        if (object != null && object instanceof Constant) {
            final Constant that = (Constant) object;
            return this.value.equals(that.value);
        }
        return false;
    }

    @Override
    protected final ReadonlyList<Expression> doGetOperandExpressionList() {
        return EMPTY_EXPRESSION_LIST;
    }

    @Override
    protected final String doToString() {
        final StringBuilder stringBuilder = new StringBuilder();
        switch (this.dataType) {
            case STRING:
                stringBuilder.append("'");
                stringBuilder.append(this.value.toString());
                stringBuilder.append("'");
                break;
            default:
                stringBuilder.append(this.value.toString());
        }
        return stringBuilder.toString();
    }

    @Override
    public final DataType getResultType() throws SQL4ESException {
        return DataType.getDataType(this.value.getClass());
    }

}
