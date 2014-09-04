package com.code.crabs.jdbc.lang.expression;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.core.DataType;
import com.code.crabs.core.Identifier;
import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.expression.context.Context;
import com.code.crabs.exception.SQL4ESException;

public final class Reference extends Expression {

    public static final Identifier ALL_COLUMN_IDENTIFIER = new Identifier("*");

    public Reference(final String setIdentifier,
                     final String columnIdentifier) {
        if (columnIdentifier == null) {
            throw new IllegalArgumentException("Argument[columnIdentifier] is null.");
        }
        this.setIdentifier = setIdentifier == null ? null : new Identifier(setIdentifier);
        this.columnIdentifier = new Identifier(columnIdentifier);
        this.index = null;
        this.dataType = null;
    }

    public Reference(final String setIdentifier,
                     final Identifier columnIdentifier) {
        if (columnIdentifier == null) {
            throw new IllegalArgumentException("Argument[columnIdentifier] is null.");
        }
        this.setIdentifier = setIdentifier == null ? null : new Identifier(setIdentifier);
        this.columnIdentifier = columnIdentifier;
        this.index = null;
        this.dataType = null;
    }

    public Reference(final Identifier setIdentifier,
                     final Identifier columnIdentifier) {
        if (columnIdentifier == null) {
            throw new IllegalArgumentException("Argument[columnIdentifier] is null.");
        }
        this.setIdentifier = setIdentifier;
        this.columnIdentifier = columnIdentifier;
        this.index = null;
        this.dataType = null;
    }

    public Reference(final Identifier setIdentifier,
                     final Identifier columnIdentifier, final int index,
                     final DataType dataType) {
        if (columnIdentifier == null) {
            throw new IllegalArgumentException("Argument[columnIdentifier] is null.");
        }
        this.setIdentifier = setIdentifier;
        this.columnIdentifier = columnIdentifier;
        this.index = index;
        this.dataType = dataType;
    }

    public final Identifier setIdentifier;

    public final Identifier columnIdentifier;

    public final Integer index;

    public final DataType dataType;

    @Override
    public final int hashCode() {
        return this.setIdentifier == null ? this.columnIdentifier.hashCode()
                : 31 * this.setIdentifier.hashCode()
                + this.columnIdentifier.hashCode();
    }

    @Override
    public final boolean equals(final Object object) {
        if (object != null && object instanceof Reference) {
            final Reference that = (Reference) object;
            if (this.setIdentifier == null) {
                return that.setIdentifier == null
                        && this.columnIdentifier.equals(that.columnIdentifier);
            } else {
                return this.setIdentifier.equals(that.setIdentifier)
                        && this.columnIdentifier.equals(that.columnIdentifier);
            }
        } else {
            return false;
        }
    }

    @Override
    protected final ReadonlyList<Expression> doGetOperandExpressionList() {
        return EMPTY_EXPRESSION_LIST;
    }

    @Override
    protected final String doToString() {
        return this.setIdentifier == null ? "" + this.columnIdentifier.toString()
                : this.setIdentifier.toString() + "." + this.columnIdentifier.toString();
    }

    @Override
    public final DataType getResultType() throws SQL4ESException {
        return null;
    }

    @Override
    public final DataType getResultType(final Context context) throws SQL4ESException {
        final Object value = context.getReferenceValue(this);
        return DataType.getDataType(value.getClass());
    }

}
