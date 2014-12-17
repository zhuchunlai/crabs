package org.codefamily.crabs.jdbc.compiler.extension.statement;

import org.codefamily.crabs.util.ReadonlyList;
import org.codefamily.crabs.jdbc.lang.Clause;
import org.codefamily.crabs.jdbc.lang.Keyword;
import org.codefamily.crabs.jdbc.lang.Statement;
import org.codefamily.crabs.jdbc.lang.extension.ReservedKeyword;
import org.codefamily.crabs.jdbc.lang.extension.statement.SelectStatement;
import org.codefamily.crabs.jdbc.compiler.StatementFactory;
import org.codefamily.crabs.jdbc.lang.extension.clause.*;

import java.sql.SQLException;

public final class SelectStatementAdapter extends StatementFactory.StatementAdapter {

    public SelectStatementAdapter() {
        super(SelectClause.class);
    }

    @Override
    protected final Statement adaptStatement(final ReadonlyList<Clause> clauseList) throws SQLException {
        SelectClause selectClause = null;
        FromClause fromClause = null;
        WhereClause whereClause = null;
        GroupByClause groupByClause = null;
        HavingClause havingClause = null;
        OrderByClause orderByClause = null;
        LimitClause limitClause = null;
        for (int i = 0, clauseCount = clauseList.size(); i < clauseCount; i++) {
            final Clause clause = clauseList.get(i);
            if (clause.prefixKeywordList.isEmpty()) {
                throw new SQLException("Unexpected clause in select statement. " + clause.prefixKeywordList);
            }
            final Keyword keyword = clause.prefixKeywordList.get(0);
            if (keyword == ReservedKeyword.SELECT) {
                if (selectClause != null) {
                    throw new SQLException("Conflict select clause in select statement.");
                }
                selectClause = (SelectClause) clause;
            } else if (keyword == ReservedKeyword.FROM) {
                if (fromClause != null) {
                    throw new SQLException("Conflict from clause in select statement.");
                }
                fromClause = (FromClause) clause;
            } else if (keyword == ReservedKeyword.WHERE) {
                if (whereClause != null) {
                    throw new SQLException("Conflict where clause in select statement.");
                }
                whereClause = (WhereClause) clause;
            } else if (keyword == ReservedKeyword.GROUP) {
                if (groupByClause != null) {
                    throw new SQLException("Conflict group by clause in select statement.");
                }
                groupByClause = (GroupByClause) clause;
            } else if (keyword == ReservedKeyword.HAVING) {
                if (havingClause != null) {
                    throw new SQLException("Conflict having clause in select statement.");
                }
                havingClause = (HavingClause) clause;
            } else if (keyword == ReservedKeyword.ORDER) {
                if (orderByClause != null) {
                    throw new SQLException("Conflict order by clause in select statement.");
                }
                orderByClause = (OrderByClause) clause;
            } else if (keyword == ReservedKeyword.LIMIT) {
                if (limitClause != null) {
                    throw new SQLException("Conflict limit clause in select statement.");
                }
                limitClause = (LimitClause) clause;
            } else {
                throw new SQLException("Unexpected clause in select statement. " + clause.prefixKeywordList);
            }
        }
        if (selectClause == null) {
            throw new SQLException("Missing select clause in select statement.");
        }
        if (fromClause == null) {
            throw new SQLException("Missing from clause in select statement.");
        }
        // todo 控制子句的先后顺序
        return new SelectStatement(
                selectClause,
                fromClause,
                whereClause,
                groupByClause,
                havingClause,
                orderByClause,
                limitClause
        );
    }

}
