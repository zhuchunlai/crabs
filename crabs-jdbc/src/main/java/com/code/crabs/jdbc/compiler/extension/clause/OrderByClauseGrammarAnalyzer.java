package com.code.crabs.jdbc.compiler.extension.clause;

import com.code.crabs.jdbc.lang.Expression;
import com.code.crabs.jdbc.lang.Keyword;
import com.code.crabs.jdbc.lang.extension.ReservedKeyword;
import com.code.crabs.jdbc.lang.extension.clause.OrderByClause;
import com.code.crabs.jdbc.lang.extension.clause.OrderByClause.OrderSpecification;
import com.code.crabs.jdbc.compiler.GrammarAnalyzer.ClauseGrammarAnalyzer;

import java.sql.SQLException;
import java.util.ArrayList;

public final class OrderByClauseGrammarAnalyzer extends ClauseGrammarAnalyzer {

    public OrderByClauseGrammarAnalyzer() {
        super(OrderByClause.PREFIX_KEYWORD_LIST);
    }

    @Override
    protected final OrderByClause doAnalyze(final GrammarAnalyzeContext context) throws SQLException {
        return new OrderByClause(analyzeOrderSpecifications(context));
    }

    private static OrderSpecification[] analyzeOrderSpecifications(final GrammarAnalyzeContext context)
            throws SQLException {
        final ArrayList<OrderSpecification> orderSpecificationList = new ArrayList<OrderSpecification>(1);
        for (; ; ) {
            final int currentTokenStartPosition = context.currentTokenStartPosition();
            final OrderSpecification orderSpecification = analyzeOrderSpecification(context);
            if (orderSpecification == null) {
                throw newSQLException(
                        context,
                        "Expect a order specification.",
                        currentTokenStartPosition
                );
            }
            orderSpecificationList.add(orderSpecification);
            if (context.currentTokenType() == TokenType.SYMBOL && context.currentTokenToSymbol() == ',') {
                context.toNextToken();
                continue;
            }
            break;
        }
        return orderSpecificationList.toArray(new OrderSpecification[orderSpecificationList.size()]);
    }

    private static OrderSpecification analyzeOrderSpecification(final GrammarAnalyzeContext context)
            throws SQLException {
        final Expression expression = ExpressionGrammarAnalyzer.analyze(context);
        if (expression == null) {
            return null;
        }
        final boolean ascendingOrder;
        final boolean nullsFirst;
        if (context.currentTokenType() == TokenType.KEYWORD) {
            Keyword keyword = context.currentTokenToKeyword();
            if (keyword == ReservedKeyword.ASC) {
                ascendingOrder = true;
                context.toNextToken();
            } else if (keyword == ReservedKeyword.DESC) {
                ascendingOrder = false;
                context.toNextToken();
            } else {
                ascendingOrder = true;
            }
        } else {
            ascendingOrder = true;
        }
        return new OrderSpecification(expression, ascendingOrder);
    }

}
