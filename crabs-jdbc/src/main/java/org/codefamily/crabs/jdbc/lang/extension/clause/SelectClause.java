package org.codefamily.crabs.jdbc.lang.extension.clause;

import org.codefamily.crabs.util.ReadonlyList;
import org.codefamily.crabs.core.Identifier;
import org.codefamily.crabs.jdbc.lang.Clause;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.Keyword;
import org.codefamily.crabs.jdbc.lang.extension.ReservedKeyword;

import static org.codefamily.crabs.Constants.WHITESPACE;

public final class SelectClause extends Clause {

    public static final ReadonlyList<Keyword> PREFIX_KEYWORD_LIST
            = ReadonlyList.newInstance((Keyword) ReservedKeyword.SELECT);

    public SelectClause(final Boolean distinct,
                        final Expression topNExpression,
                        final ResultColumnDeclare... resultColumnDeclares) {
        super(PREFIX_KEYWORD_LIST);
        if (resultColumnDeclares == null) {
            throw new IllegalArgumentException("Argument[resultColumnDeclares] is null.");
        }
        if (resultColumnDeclares.length == 0) {
            throw new IllegalArgumentException("Argument[resultColumnDeclares] is empty.");
        }
        for (int i = 0; i < resultColumnDeclares.length; i++) {
            if (resultColumnDeclares[i] == null) {
                throw new IllegalArgumentException("Argument[resultColumnDeclares[" + i + "]] is null.");
            }
        }
        this.distinct = distinct;
        this.topNExpression = topNExpression;
        this.resultColumnDeclareList = ReadonlyList.newInstance(resultColumnDeclares);
    }

    public final Boolean distinct;

    public final Expression topNExpression;

    public final ReadonlyList<ResultColumnDeclare> resultColumnDeclareList;

    private ReadonlyList<Expression> resultColumnExpressionList;

    public final ReadonlyList<Expression> getResultColumnExpressionList() {
        if (this.resultColumnExpressionList == null) {
            final ReadonlyList<ResultColumnDeclare> resultColumnDeclareList = this.resultColumnDeclareList;
            final int resultColumnCount = resultColumnDeclareList.size();
            final Expression[] columnExpressions = new Expression[resultColumnCount];
            for (int i = 0; i < resultColumnCount; i++) {
                columnExpressions[i] = resultColumnDeclareList.get(i).expression;
            }
            this.resultColumnExpressionList = ReadonlyList.newInstance(columnExpressions);
        }
        return this.resultColumnExpressionList;
    }

    @Override
    public final String toString() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.getPrefixKeywordsString());
        if (this.distinct != null && this.distinct) {
            stringBuilder.append(WHITESPACE);
            stringBuilder.append(ReservedKeyword.DISTINCT.getName());
        }
        if (this.topNExpression != null) {
            stringBuilder.append(WHITESPACE);
            stringBuilder.append(this.topNExpression.toString());
        }
        final ReadonlyList<ResultColumnDeclare> resultColumnDeclareList = this.resultColumnDeclareList;
        if (resultColumnDeclareList.size() > 0) {
            stringBuilder.append(WHITESPACE);
            stringBuilder.append(resultColumnDeclareList.get(0).toString());
            for (int i = 1, columnDeclareCount = resultColumnDeclareList.size(); i < columnDeclareCount; i++) {
                stringBuilder.append(", ");
                stringBuilder.append(resultColumnDeclareList.get(i).toString());
            }
        }
        stringBuilder.append(WHITESPACE);
        return stringBuilder.toString();
    }

    @Override
    public final boolean equals(final Object object) {
        if (object != null && object instanceof SelectClause) {
            final SelectClause that = (SelectClause) object;
            final ReadonlyList<ResultColumnDeclare> thisResultColumnDeclareList = this.resultColumnDeclareList;
            final ReadonlyList<ResultColumnDeclare> thatResultColumnDeclareList = that.resultColumnDeclareList;
            if (thisResultColumnDeclareList.size() == thatResultColumnDeclareList.size()) {
                for (int i = 0, thisColumnDeclareCount = thisResultColumnDeclareList.size();
                     i < thisColumnDeclareCount; i++) {
                    if (!thisResultColumnDeclareList.get(i).equals(thatResultColumnDeclareList.get(i))) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public static final class ResultColumnDeclare {

        public ResultColumnDeclare(final String alias,
                                   final Expression expression) {
            if (expression == null) {
                throw new IllegalArgumentException("Argument[expression] is null.");
            }
            this.alias = alias == null ? null : new Identifier(alias);
            this.expression = expression;
        }

        public ResultColumnDeclare(final Identifier alias,
                                   final Expression expression) {
            if (expression == null) {
                throw new IllegalArgumentException("Argument[expression] is null.");
            }
            this.alias = alias;
            this.expression = expression;
        }

        public final Identifier alias;

        public final Expression expression;

        @Override
        public final String toString() {
            return this.alias == null ? this.expression.toString()
                    : this.expression.toString() + " AS "
                    + this.alias.toString();
        }

        @Override
        public final boolean equals(final Object object) {
            if (object != null && object instanceof ResultColumnDeclare) {
                final ResultColumnDeclare that = (ResultColumnDeclare) object;
                if (this.alias == null) {
                    return this.expression.equals(that.expression)
                            && that.alias == null;
                } else {
                    return this.expression.equals(that.expression)
                            && this.alias.equals(that.alias);
                }
            }
            return false;
        }

    }

}
