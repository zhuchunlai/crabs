package org.codefamily.crabs.jdbc.compiler.extension.clause;

import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.extension.clause.GroupByClause;
import org.codefamily.crabs.jdbc.compiler.GrammarAnalyzer.ClauseGrammarAnalyzer;

import java.sql.SQLException;
import java.util.ArrayList;

public final class GroupByClauseGrammarAnalyzer extends ClauseGrammarAnalyzer {

    public GroupByClauseGrammarAnalyzer() {
        super(GroupByClause.PREFIX_KEYWORD_LIST);
    }

    @Override
    protected final GroupByClause doAnalyze(final GrammarAnalyzeContext context) throws SQLException {
        final ArrayList<Expression> groupByExpressionList = context.getExpressionList();
        final int startListIndex = groupByExpressionList.size();
        for (; ; ) {
            final int currentTokenStartPosition = context
                    .currentTokenStartPosition();
            final Expression expression = ExpressionGrammarAnalyzer.analyze(context);
            if (expression != null) {
                groupByExpressionList.add(expression);
                if (context.currentTokenType() == TokenType.SYMBOL
                        && context.currentTokenToSymbol() == ',') {
                    context.toNextToken();
                    continue;
                }
                break;
            } else {
                throw newSQLException(context, "Expect an expression.", currentTokenStartPosition);
            }
        }
        return new GroupByClause(expressionsListToArray(groupByExpressionList, startListIndex));
    }

}
