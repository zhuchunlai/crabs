package org.codefamily.crabs.jdbc.compiler.extension.clause;

import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.Keyword;
import org.codefamily.crabs.jdbc.lang.extension.ReservedKeyword;
import org.codefamily.crabs.jdbc.lang.extension.clause.SelectClause;
import org.codefamily.crabs.jdbc.lang.extension.clause.SelectClause.ResultColumnDeclare;
import org.codefamily.crabs.jdbc.compiler.GrammarAnalyzer.ClauseGrammarAnalyzer;

import java.sql.SQLException;
import java.util.ArrayList;

public final class SelectClauseGrammarAnalyzer extends ClauseGrammarAnalyzer {

    public SelectClauseGrammarAnalyzer() {
        super(SelectClause.PREFIX_KEYWORD_LIST);
    }

    @Override
    protected final SelectClause doAnalyze(final GrammarAnalyzeContext context) throws SQLException {
        Boolean distinct = null;
        Expression topNExpression = null;
        for (; ; ) {
            if (context.currentTokenType() == TokenType.KEYWORD) {
                final Keyword keyword = context.currentTokenToKeyword();
                if (keyword == ReservedKeyword.DISTINCT) {
                    /*if (distinct == null) {
                        context.toNextToken();
                        distinct = true;
                        continue;
                    } else {
                        throw new SQLException("Conflict distinct declare in select clause.");
                    }*/
                    // TODO 暂时不支持DISTINCT
                    throw new SQLException("Now, distinct is not supported in select clause.");
                } else if (keyword == ReservedKeyword.TOP) {
                    /*if (topNExpression == null) {
                        context.toNextToken();
                        final int currentTokenStartPosition = context.currentTokenStartPosition();
                        topNExpression = ExpressionGrammarAnalyzer.analyze(context);
                        if (topNExpression == null) {
                            throw newSQLException(
                                    context,
                                    "Expect an expression.",
                                    currentTokenStartPosition
                            );
                        }
                        continue;
                    } else {
                        throw new SQLException("Conflict top N declare in select clause.");
                    }*/
                    // TODO 暂时不支持TOP N
                    throw new SQLException("Now, top N is not supported in select clause.");
                }
            }
            break;
        }
        return new SelectClause(distinct, topNExpression, analyzeResultColumnDeclares(context));
    }

    private static ResultColumnDeclare[] analyzeResultColumnDeclares(
            final GrammarAnalyzeContext context) throws SQLException {
        final ArrayList<ResultColumnDeclare> columnDeclareList = new ArrayList<ResultColumnDeclare>();
        for (; ; ) {
            final int currentTokenStartPosition = context.currentTokenStartPosition();
            final ResultColumnDeclare columnDeclare = analyzeColumnDeclare(context);
            if (columnDeclare == null) {
                throw newSQLException(
                        context,
                        "Expect a column declare of result set.",
                        currentTokenStartPosition
                );
            }
            columnDeclareList.add(columnDeclare);
            if (context.currentTokenType() == TokenType.SYMBOL
                    && context.currentTokenToSymbol() == ',') {
                context.toNextToken();
                continue;
            }
            break;
        }
        return columnDeclareList.toArray(new ResultColumnDeclare[columnDeclareList.size()]);
    }

    private static ResultColumnDeclare analyzeColumnDeclare(
            final GrammarAnalyzeContext context) throws SQLException {
        final Expression expression = ExpressionGrammarAnalyzer.analyze(context);
        if (expression == null) {
            return null;
        }
        final String alias;
        if (context.currentTokenType() == TokenType.KEYWORD
                && context.currentTokenToKeyword() == ReservedKeyword.AS) {
            context.toNextToken();
            final int currentTokenStartPosition = context.currentTokenStartPosition();
            alias = analyzeGeneralizedIdentifier(context);
            if (alias == null) {
                throw newSQLException(
                        context,
                        "Expect a column declare alias.",
                        currentTokenStartPosition
                );
            }
        } else {
            alias = analyzeGeneralizedIdentifier(context);
        }
        return new ResultColumnDeclare(alias, expression);
    }

}
