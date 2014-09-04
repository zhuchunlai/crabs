package com.code.crabs.jdbc.lang.extension.statement;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.exception.SQL4ESException;
import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.Statement;
import com.code.crabs.jdbc.lang.expression.util.ExpressionHelper;
import com.code.crabs.jdbc.lang.extension.clause.*;

public final class SelectStatement extends Statement {

    public SelectStatement(final SelectClause selectClause,
                           final FromClause fromClause,
                           final WhereClause whereClause,
                           final GroupByClause groupByClause,
                           final HavingClause havingClause,
                           final OrderByClause orderByClause,
                           final LimitClause limitClause) {
        if (selectClause == null) {
            throw new IllegalArgumentException("Argument[selectClause] is null.");
        }
        if (fromClause == null) {
            throw new IllegalArgumentException("Argument[fromClause] is null.");
        }
        this.selectClause = selectClause;
        this.fromClause = fromClause;
        this.whereClause = whereClause;
        this.groupByClause = groupByClause;
        this.havingClause = havingClause;
        this.orderByClause = orderByClause;
        this.limitClause = limitClause;
    }

    public final SelectClause selectClause;

    public final FromClause fromClause;

    public final WhereClause whereClause;

    public final GroupByClause groupByClause;

    public final HavingClause havingClause;

    public final OrderByClause orderByClause;

    public final LimitClause limitClause;

    @Override
    public final boolean equals(final Object object) {
        if (object != null && object instanceof SelectStatement) {
            if (object == this) {
                return true;
            }
            final SelectStatement that = (SelectStatement) object;
            return this.selectClause.equals(that.selectClause)
                    && this.fromClause.equals(that.fromClause)
                    && (this.whereClause == null ? that.whereClause == null
                    : this.whereClause.equals(that.whereClause))
                    && (this.groupByClause == null ? that.groupByClause == null
                    : this.groupByClause.equals(that.groupByClause))
                    && (this.havingClause == null ? that.havingClause == null
                    : this.havingClause.equals(that.havingClause))
                    && (this.orderByClause == null ? that.orderByClause == null
                    : this.orderByClause.equals(that.orderByClause))
                    && (this.limitClause == null ? that.limitClause == null
                    : this.limitClause.equals(that.limitClause));
        }
        return false;
    }

    private ReadonlyList<Expression> topLevelExpressionListBaseSourceSet;

    private ReadonlyList<Expression> topLevelExpressionListBaseResultSet;

    public final ReadonlyList<Expression> getTopLevelExpressionListBaseSourceSet() {
        if (this.topLevelExpressionListBaseSourceSet == null) {
            this.topLevelExpressionListBaseSourceSet = ReadonlyList
                    .newInstance(ExpressionHelper
                            .concatExpressionArrays(
                                    this.selectClause
                                            .getResultColumnExpressionList()
                                            .toArray(
                                                    Expression.EMPTY_EXPRESSIONS),
                                    this.whereClause == null ? null
                                            : new Expression[]{this.whereClause.conditionExpression},
                                    this.groupByClause == null ? null
                                            : this.groupByClause.groupExpressionList
                                            .toArray(Expression.EMPTY_EXPRESSIONS)));
        }
        return this.topLevelExpressionListBaseSourceSet;
    }

    public final ReadonlyList<Expression> getTopLevelExpressionListBaseResultSet() {
        if (this.topLevelExpressionListBaseResultSet == null) {
            this.topLevelExpressionListBaseResultSet = ReadonlyList
                    .newInstance(ExpressionHelper
                            .concatExpressionArrays(
                                    this.havingClause == null ? null
                                            : new Expression[]{this.havingClause.conditionExpression},
                                    this.orderByClause == null ? null
                                            : this.orderByClause
                                            .getOrderExpressionList()
                                            .toArray(
                                                    Expression.EMPTY_EXPRESSIONS)));
        }
        return this.topLevelExpressionListBaseResultSet;
    }

    protected final String doToString() {
        return this.selectClause.toString()
                + this.fromClause.toString()
                + (this.whereClause == null ? "" : " "
                + this.whereClause.toString())
                + (this.groupByClause == null ? "" : " "
                + this.groupByClause.toString())
                + (this.havingClause == null ? "" : " "
                + this.havingClause.toString())
                + (this.orderByClause == null ? "" : " "
                + this.orderByClause.toString())
                + (this.limitClause == null ? "" : " "
                + this.limitClause.toString());
    }

    @Override
    protected final int doGetParameterCount() throws SQL4ESException {
        int argumentCount = 0;
        for (Expression expression : this.getTopLevelExpressionList()) {
            argumentCount += ExpressionHelper.getArgumentCount(expression);
        }
        return argumentCount;
    }

    @Override
    protected final ReadonlyList<Expression> doGetTopLevelExpressionList() {
        return ReadonlyList.newInstance(
                ExpressionHelper.concatExpressionArrays(
                        this.getTopLevelExpressionListBaseSourceSet().toArray(Expression.EMPTY_EXPRESSIONS),
                        this.getTopLevelExpressionListBaseResultSet().toArray(Expression.EMPTY_EXPRESSIONS)
                )
        );
    }


}
