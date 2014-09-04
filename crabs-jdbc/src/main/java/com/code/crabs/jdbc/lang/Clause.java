package com.code.crabs.jdbc.lang;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.common.util.StringUtils;
import com.code.crabs.core.Identifier;

import static com.code.crabs.common.Constants.WHITESPACE;
import static com.code.crabs.common.Constants.EMPTY_STRING;

public abstract class Clause {

    public final ReadonlyList<Keyword> prefixKeywordList;

    protected Clause(final ReadonlyList<Keyword> prefixKeywordList) {
        if (prefixKeywordList == null) {
            throw new IllegalArgumentException("Argument[prefixKeywordList] is null.");
        }
        this.prefixKeywordList = prefixKeywordList;
    }

    @Override
    public abstract String toString();

    @Override
    public abstract boolean equals(Object object);

    protected final String getPrefixKeywordsString() {
        final ReadonlyList<Keyword> prefixKeywordList = this.prefixKeywordList;
        if (prefixKeywordList.isEmpty()) {
            return EMPTY_STRING;
        } else {
            final StringBuilder stringBuilder = new StringBuilder();
            String prefix = EMPTY_STRING;
            for (int i = 0, prefixKeywordCount = prefixKeywordList.size(); i < prefixKeywordCount; i++) {
                stringBuilder.append(prefix);
                stringBuilder.append(prefixKeywordList.get(i).getName());
                prefix = WHITESPACE;
            }
            return stringBuilder.toString();
        }
    }

    public static abstract class TableDeclare {

        public final Identifier alias;

        protected TableDeclare(final String alias) {
            this(StringUtils.isNullOrEmptyAfterTrim(alias) ? null : new Identifier(alias));
        }

        protected TableDeclare(final Identifier alias) {
            this.alias = alias;
        }

    }

    public static final class TableValuesDeclare {

        public TableValuesDeclare(final Expression... valueExpressions) {
            if (valueExpressions == null) {
                throw new IllegalArgumentException("Argument[valueExpressions] is null.");
            }
            this.valueExpressionList = ReadonlyList.newInstance(valueExpressions);
        }

        public final ReadonlyList<Expression> valueExpressionList;

        private String toStringValue;

        @Override
        public final String toString() {
            if (this.toStringValue == null) {
                final StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append('(');
                final ReadonlyList<Expression> valueExpressionList = this.valueExpressionList;
                final int valueExpressionCount = valueExpressionList.size();
                if (valueExpressionCount > 0) {
                    Expression valueExpression = valueExpressionList.get(0);
                    stringBuilder.append(valueExpression == null ? EMPTY_STRING : valueExpression.toString());
                    for (int i = 1; i < valueExpressionCount; i++) {
                        stringBuilder.append(',');
                        stringBuilder.append(WHITESPACE);
                        valueExpression = valueExpressionList.get(i);
                        stringBuilder.append(valueExpression == null ? EMPTY_STRING : valueExpression.toString());
                    }
                }
                stringBuilder.append(')');
                this.toStringValue = stringBuilder.toString();
            }
            return this.toStringValue;
        }

        @Override
        public final boolean equals(final Object object) {
            return object != null && object instanceof TableValuesDeclare
                    && this.equals(TableValuesDeclare.class.cast(object));
        }

        final boolean equals(final TableValuesDeclare that) {
            if (that == this) {
                return true;
            }
            final ReadonlyList<Expression> thisValueExpressionList = this.valueExpressionList;
            final ReadonlyList<Expression> thatValueExpressionList = that.valueExpressionList;
            if (thisValueExpressionList.size() != thatValueExpressionList.size()) {
                return false;
            }
            for (int i = 0, count = thisValueExpressionList.size(); i < count; i++) {
                if (thisValueExpressionList.get(i) == null) {
                    if (thatValueExpressionList.get(i) != null) {
                        return false;
                    }
                } else if (!thisValueExpressionList.get(i).equals(thatValueExpressionList.get(i))) {
                    return false;
                }
            }
            return true;
        }

    }

}
