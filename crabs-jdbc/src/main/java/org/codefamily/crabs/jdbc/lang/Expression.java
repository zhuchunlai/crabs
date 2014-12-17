package org.codefamily.crabs.jdbc.lang;

import org.codefamily.crabs.util.ReadonlyList;
import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.jdbc.lang.expression.context.Context;
import org.codefamily.crabs.exception.CrabsException;

public abstract class Expression {

    public static final Expression[] EMPTY_EXPRESSIONS = new Expression[0];

    public static final ReadonlyList<Expression> EMPTY_EXPRESSION_LIST = ReadonlyList.newInstance(EMPTY_EXPRESSIONS);

    protected Expression() {
        // nothing to do.
    }

    private ReadonlyList<Expression> operandExpressionList;

    public abstract DataType getResultType() throws CrabsException;

    public DataType getResultType(Context context) throws CrabsException {
        return this.getResultType();
    }

    public final ReadonlyList<Expression> getOperandExpressionList() throws CrabsException {
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
            } catch (CrabsException e) {
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
        } catch (CrabsException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected abstract ReadonlyList<Expression> doGetOperandExpressionList() throws CrabsException;

    protected abstract String doToString() throws CrabsException;

}
