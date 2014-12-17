package org.codefamily.crabs.jdbc.engine;

import org.codefamily.crabs.util.ExtensionClassCollector;
import org.codefamily.crabs.core.client.AdvancedClient;
import org.codefamily.crabs.jdbc.lang.Statement;
import org.codefamily.crabs.exception.CrabsException;

import java.util.Iterator;

public final class ExecuteEngine {

    public static <TStatement extends Statement, TResult> TResult executeStatement(
            final AdvancedClient advancedClient,
            final ExecuteEnvironment environment,
            TStatement statement,
            final Object[] argumentValues,
            final Class<TResult> resultClass) throws CrabsException {
        if (advancedClient == null) {
            throw new IllegalArgumentException("Argument[advancedClient] is null.");
        }
        if (environment == null) {
            throw new IllegalArgumentException("Argument[environment] is null.");
        }
        if (statement == null) {
            throw new IllegalArgumentException("Argument[statement] is null.");
        }
        if (argumentValues == null) {
            throw new IllegalArgumentException("Argument[argumentValues] is null.");
        }
        if (resultClass == null) {
            throw new IllegalArgumentException("Argument[resultClass] is null.");
        }
        @SuppressWarnings("unchecked")
        final StatementExecutor<TStatement, TResult> statementExecutor
                = STATEMENT_EXECUTOR_SET.getStatementExecutor((Class<TStatement>) (statement.getClass()), resultClass);
        if (statementExecutor == null) {
            throw new CrabsException("Can not found statement execute engine for "
                    + statement.getClass().getSimpleName()
                    + ", result class " + resultClass.getName());
        }
        return statementExecutor.execute(advancedClient, statement, environment, argumentValues);
    }

    public static void recollectStatementExecutor() {
        synchronized (ExecuteEngine.class) {
            @SuppressWarnings("rawtypes")
            final Iterator<Class<? extends StatementExecutor>> statementExecutorClassIterator
                    = ExtensionClassCollector.getExtensionClasses(StatementExecutor.class);
            try {
                while (statementExecutorClassIterator.hasNext()) {
                    @SuppressWarnings("rawtypes")
                    final Class<? extends StatementExecutor> statementExecutorClass
                            = statementExecutorClassIterator.next();
                    if (STATEMENT_EXECUTOR_SET.registerStatementExecutor(statementExecutorClass)
                            != statementExecutorClass) {
                        throw new RuntimeException(
                                "Statement executor conflicted." + statementExecutorClass.getName()
                        );
                    }
                }
            } catch (CrabsException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final StatementExecutorSet STATEMENT_EXECUTOR_SET = new StatementExecutorSet(10);

    static {
        recollectStatementExecutor();
    }

    private ExecuteEngine() {
        // to do nothing.
    }

    private static final class StatementExecutorSet {

        StatementExecutorSet(final int capacity) {
            this.map = new StatementExecutor[capacity];
        }

        private final StatementExecutor<?, ?>[] map;

        @SuppressWarnings("unchecked")
        final <TStatement extends Statement, TResult> StatementExecutor<TStatement, TResult> getStatementExecutor(
                final Class<TStatement> statementClass,
                final Class<TResult> resultClass) {
            StatementExecutor<?, ?> currentExecutor = this.map[this.hashIndexOf(statementClass, resultClass)];
            while (currentExecutor != null) {
                if (currentExecutor.statementClass == statementClass
                        && currentExecutor.resultClass == resultClass) {
                    break;
                } else {
                    currentExecutor = currentExecutor.nextInSet;
                }
            }
            return (StatementExecutor<TStatement, TResult>) currentExecutor;
        }

        @SuppressWarnings("rawtypes")
        final Class<? extends StatementExecutor> registerStatementExecutor(
                final Class<? extends StatementExecutor> statementExecutorClass)
                throws CrabsException {
            final StatementExecutor<?, ?> statementExecutor;
            try {
                statementExecutor = statementExecutorClass.newInstance();
            } catch (InstantiationException e) {
                throw new CrabsException(e);
            } catch (IllegalAccessException e) {
                throw new CrabsException(e);
            }
            final StatementExecutor<?, ?> existedStatementExecutor
                    = getStatementExecutor(statementExecutor.statementClass, statementExecutor.resultClass);
            if (existedStatementExecutor != null) {
                return existedStatementExecutor.getClass();
            } else {
                final int hashIndex = this.hashIndexOf(
                        statementExecutor.statementClass,
                        statementExecutor.resultClass
                );
                statementExecutor.nextInSet = this.map[hashIndex];
                this.map[hashIndex] = statementExecutor;
                return statementExecutorClass;
            }
        }

        private int hashIndexOf(
                final Class<? extends Statement> statementClass,
                final Class<?> resultClass) {
            return (31 * statementClass.hashCode() + resultClass.hashCode())
                    & (this.map.length - 1);
        }

    }

}
