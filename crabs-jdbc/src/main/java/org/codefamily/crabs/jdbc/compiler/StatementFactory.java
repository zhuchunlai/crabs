package org.codefamily.crabs.jdbc.compiler;

import org.codefamily.crabs.util.ExtensionClassCollector;
import org.codefamily.crabs.util.ReadonlyList;
import org.codefamily.crabs.jdbc.lang.Clause;
import org.codefamily.crabs.jdbc.lang.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public final class StatementFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatementFactory.class);

    private static final HashMap<Class<? extends Clause>, StatementAdapter> STATEMENT_ADAPTER_MAP
            = new HashMap<Class<? extends Clause>, StatementAdapter>();

    public static void recollectStatementAdapters() {
        synchronized (StatementFactory.class) {
            final Iterator<Class<? extends StatementAdapter>> adapterClassIterator
                    = ExtensionClassCollector.getExtensionClasses(StatementAdapter.class);
            while (adapterClassIterator.hasNext()) {
                final Class<? extends StatementAdapter> adapterClass = adapterClassIterator
                        .next();
                try {
                    final StatementAdapter statementAdapter = adapterClass.newInstance();
                    final Class<? extends Clause> key = statementAdapter.markClauseClass;
                    final StatementAdapter existedStatementAdapter = STATEMENT_ADAPTER_MAP.get(key);
                    if (existedStatementAdapter != null) {
                        if (existedStatementAdapter.getClass() != adapterClass) {
                            throw new RuntimeException("Statement adapter conflicted." + adapterClass.getName());
                        }
                        continue;
                    }
                    STATEMENT_ADAPTER_MAP.put(key, statementAdapter);
                } catch (Throwable exception) {
                    LOGGER.error("Can not register statement adapter.", exception);
                }
            }
        }
    }

    static Statement toStatement(final List<Clause> clauseList) throws SQLException {
        final ReadonlyList<Clause> readonlyClauseList = ReadonlyList.newInstance(clauseList);
        for (int i = 0, clauseCount = readonlyClauseList.size(); i < clauseCount; i++) {
            final StatementAdapter statementAdapter = STATEMENT_ADAPTER_MAP.get(readonlyClauseList.get(i).getClass());
            if (statementAdapter != null) {
                return statementAdapter.adaptStatement(readonlyClauseList);
            }
        }
        return null;
    }

    static {
        recollectStatementAdapters();
    }

    private StatementFactory() {
        // to do nothing.
    }

    public static abstract class StatementAdapter {

        protected StatementAdapter(final Class<? extends Clause> markClauseClass) {
            if (markClauseClass == null) {
                throw new IllegalArgumentException("Argument [markClauseClass] is null.");
            }
            this.markClauseClass = markClauseClass;
        }

        final Class<? extends Clause> markClauseClass;

        protected abstract Statement adaptStatement(ReadonlyList<Clause> clauseList) throws SQLException;

    }

}
