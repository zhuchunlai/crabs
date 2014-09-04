package com.code.crabs.jdbc.lang;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.core.DataType;
import com.code.crabs.jdbc.lang.expression.context.Context;
import com.code.crabs.exception.SQL4ESException;

public abstract class Expression {

    public static final Expression[] EMPTY_EXPRESSIONS = new Expression[0];

    public static final ReadonlyList<Expression> EMPTY_EXPRESSION_LIST = ReadonlyList.newInstance(EMPTY_EXPRESSIONS);

    protected Expression() {
        // nothing to do.
    }

    private ReadonlyList<Expression> operandExpressionList;

    public abstract DataType getResultType() throws SQL4ESException;

    public DataType getResultType(Context context) throws SQL4ESException {
        return this.getResultType();
    }

    public final ReadonlyList<Expression> getOperandExpressionList() throws SQL4ESException {
        if (this.operandExpressionList == null) {
            this.operandExpressionList = this.doGetOperandExpressionList();
        }
        return this.operandExpressionList;
    }

    private Integer hashCode;

    @Override
    public int hashCode() {
        if (this.hashCode == null) {
            this.hashCode = this.toString().hashCode();
        }
        return this.hashCode;
    }

    private String string;

    @Override
    public final String toString() {
        if (this.string == null) {
            try {
                this.string = this.doToString();
            } catch (SQL4ESException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return this.string;
    }

    @Override
    public boolean equals(final Object object) {
        try {
            if (object != null && object.getClass() == this.getClass()) {
                if (object == this) {
                    return true;
                }
                final ReadonlyList<Expression> thisOperandExpressionList = this.getOperandExpressionList();
                final ReadonlyList<Expression> thatOperandExpressionList
                        = ((Expression) object).getOperandExpressionList();
                if (thisOperandExpressionList.size() == thatOperandExpressionList.size()) {
                    for (int i = 0, thisOperandExpressionCount = thisOperandExpressionList.size();
                         i < thisOperandExpressionCount; i++) {
                        if (!thisOperandExpressionList.get(i).equals(thatOperandExpressionList.get(i))) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        } catch (SQL4ESException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected abstract ReadonlyList<Expression> doGetOperandExpressionList() throws SQL4ESException;

    protected abstract String doToString() throws SQL4ESException;

}
