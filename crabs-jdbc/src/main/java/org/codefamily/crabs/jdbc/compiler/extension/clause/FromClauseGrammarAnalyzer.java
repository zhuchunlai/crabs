package org.codefamily.crabs.jdbc.compiler.extension.clause;

import org.codefamily.crabs.jdbc.lang.Clause.TableDeclare;
import org.codefamily.crabs.jdbc.lang.extension.clause.FromClause;
import org.codefamily.crabs.jdbc.compiler.GrammarAnalyzer.ClauseGrammarAnalyzer;

import java.sql.SQLException;
import java.util.ArrayList;

public final class FromClauseGrammarAnalyzer extends ClauseGrammarAnalyzer {

    public FromClauseGrammarAnalyzer() {
        super(FromClause.PREFIX_KEYWORD_LIST);
    }

    @Override
    protected final FromClause doAnalyze(final GrammarAnalyzeContext context) throws SQLException {
        return new FromClause(analyzeTableDeclares(context));
    }

    private static TableDeclare[] analyzeTableDeclares(final GrammarAnalyzeContext context) throws SQLException {
        final ArrayList<TableDeclare> tableDeclareList = new ArrayList<TableDeclare>();
        for (; ; ) {
            final int currentTokenStartPosition = context.currentTokenStartPosition();
            final TableDeclare tableDeclare = analyzeTableDeclare(context);
            if (tableDeclare == null) {
                throw newSQLException(context, "Expect a table declare.", currentTokenStartPosition);
            }
            tableDeclareList.add(tableDeclare);
            if (context.currentTokenType() == TokenType.SYMBOL
                    && context.currentTokenToSymbol() == ',') {
                /*context.toNextToken();
                continue;*/
                // TODO 暂不支持多表关联
                throw newSQLException(context, "Now only support a single table declare.", currentTokenStartPosition);
            }
            break;
        }
        return tableDeclareList.toArray(new TableDeclare[tableDeclareList.size()]);
    }

}
