package org.codefamily.crabs.jdbc.compiler.extension.clause;

import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.expression.Constant;
import org.codefamily.crabs.jdbc.lang.extension.clause.LimitClause;
import org.codefamily.crabs.jdbc.compiler.GrammarAnalyzer.ClauseGrammarAnalyzer;

import java.sql.SQLException;

public final class LimitClauseGrammarAnalyzer extends ClauseGrammarAnalyzer {

    public LimitClauseGrammarAnalyzer() {
        super(LimitClause.PREFIX_KEYWORD_LIST);
    }

    @Override
    protected final LimitClause doAnalyze(final GrammarAnalyzeContext context) throws SQLException {
        final Expression offset, rowCount;
        switch (context.currentTokenType()) {
            case NUMBERS:
                offset = this.tryToParseConstant(context);
                context.toNextToken();
                if (context.currentTokenType() == TokenType.SYMBOL
                        && context.currentTokenToSymbol() == ',') {
                    context.toNextToken();
                    switch (context.currentTokenType()) {
                        case NUMBERS:
                            rowCount = this.tryToParseConstant(context);
                            context.toNextToken();
                            return new LimitClause(offset, rowCount);
                        case SYMBOL:
                            if (context.currentTokenToSymbol() == '?') {
                                rowCount = context.newArgument();
                                context.toNextToken();
                                return new LimitClause(offset, rowCount);
                            }
                            throw newSQLException(
                                    context,
                                    "Expect NUMBERS or '?'",
                                    context.currentTokenStartPosition()
                            );
                        default:
                            throw newSQLException(
                                    context,
                                    "Expect NUMBERS or '?'",
                                    context.currentTokenStartPosition()
                            );
                    }
                }
                throw newSQLException(context, "Expect symbol ','", context.currentTokenStartPosition());
            case SYMBOL:
                if (context.currentTokenToSymbol() == '?') {
                    offset = context.newArgument();
                    context.toNextToken();
                    if (context.currentTokenType() == TokenType.SYMBOL
                            && context.currentTokenToSymbol() == ',') {
                        context.toNextToken();
                        switch (context.currentTokenType()) {
                            case NUMBERS:
                                rowCount = this.tryToParseConstant(context);
                                context.toNextToken();
                                return new LimitClause(offset, rowCount);
                            case SYMBOL:
                                if (context.currentTokenToSymbol() == '?') {
                                    rowCount = context.newArgument();
                                    context.toNextToken();
                                    return new LimitClause(offset, rowCount);
                                }
                                throw newSQLException(
                                        context,
                                        "Expect NUMBERS or '?'",
                                        context.currentTokenStartPosition()
                                );
                            default:
                                throw newSQLException(
                                        context,
                                        "Expect NUMBERS or '?'",
                                        context.currentTokenStartPosition()
                                );
                        }
                    }
                }
                throw newSQLException(context, "Expect NUMBERS or '?'", context.currentTokenStartPosition());
            default:
                throw newSQLException(context, "Expect NUMBERS or '?'", context.currentTokenStartPosition());
        }
    }

    private Constant tryToParseConstant(final GrammarAnalyzeContext context) throws SQLException {
        final String tokenValue = context.currentTokenToString();
        final long value = Long.parseLong(tokenValue);
        if (value > Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return new Constant((int) value);
        } else {
            throw newSQLException(
                    context,
                    "Unsupported number."
                            + tokenValue, context.currentTokenStartPosition()
            );
        }
    }

}
