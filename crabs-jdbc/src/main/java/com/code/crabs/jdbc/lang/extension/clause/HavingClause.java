package com.code.crabs.jdbc.lang.extension.clause;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.jdbc.lang.Clause;
import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.Keyword;
import com.code.crabs.jdbc.lang.extension.ReservedKeyword;

public final class HavingClause extends Clause {

    public static final ReadonlyList<Keyword> PREFIX_KEYWORD_LIST
            = ReadonlyList.newInstance((Keyword) ReservedKeyword.HAVING);

    public HavingClause(final Expression expression) {
        super(PREFIX_KEYWORD_LIST);
        if (expression == null) {
            throw new IllegalArgumentException("Argument[expressions] is null.");
        }
        this.conditionExpression = expression;
    }

    public final Expression conditionExpression;

    @Override
    public final String toString() {
        return this.getPrefixKeywordsString() + " "
                + this.conditionExpression.toString();
    }

    @Override
    public final boolean equals(final Object object) {
        if (object != null && object instanceof HavingClause) {
            if (object == this) {
                return true;
            }
            final HavingClause that = (HavingClause) object;
            return this.conditionExpression.equals(that.conditionExpression);
        }
        return false;
    }

}
