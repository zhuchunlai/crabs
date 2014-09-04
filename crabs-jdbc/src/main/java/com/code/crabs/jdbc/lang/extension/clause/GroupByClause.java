package com.code.crabs.jdbc.lang.extension.clause;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.jdbc.lang.Clause;
import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.Keyword;
import com.code.crabs.jdbc.lang.extension.ReservedKeyword;

public final class GroupByClause extends Clause {

    public static final ReadonlyList<Keyword> PREFIX_KEYWORD_LIST
            = ReadonlyList.newInstance((Keyword) ReservedKeyword.GROUP, (Keyword) ReservedKeyword.BY);

    public GroupByClause(final Expression... expressions) {
        super(PREFIX_KEYWORD_LIST);
        if (expressions == null) {
            throw new IllegalArgumentException("Argument[expressions] is null.");
        }
        for (int i = 0; i < expressions.length; i++) {
            if (expressions[i] == null) {
                throw new IllegalArgumentException("Argument[expression[" + i + "]] is null.");
            }
        }
        this.groupExpressionList = ReadonlyList.newInstance(expressions.clone());
    }

    public final ReadonlyList<Expression> groupExpressionList;

    @Override
    public final String toString() {
        final ReadonlyList<Expression> groupExpressionList = this.groupExpressionList;
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.getPrefixKeywordsString());
        if (groupExpressionList.size() > 0) {
            stringBuilder.append(' ');
            stringBuilder.append(groupExpressionList.get(0).toString());
            for (int i = 1, groupExpressionCount = groupExpressionList.size(); i < groupExpressionCount; i++) {
                stringBuilder.append(", ");
                stringBuilder.append(groupExpressionList.get(i).toString());
            }
        }
        return stringBuilder.toString();
    }

    @Override
    public final boolean equals(final Object object) {
        if (object != null && object instanceof GroupByClause) {
            if (object == this) {
                return true;
            }
            final GroupByClause that = (GroupByClause) object;
            final ReadonlyList<Expression> thisGroupExpressionList = this.groupExpressionList;
            final ReadonlyList<Expression> thatGroupExpressionList = that.groupExpressionList;
            if (thisGroupExpressionList.size() == thatGroupExpressionList.size()) {
                for (int i = 0, thisGroupExpressionCount = thisGroupExpressionList.size();
                     i < thisGroupExpressionCount; i++) {
                    if (!thisGroupExpressionList.get(i).equals(thatGroupExpressionList.get(i))) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

}
