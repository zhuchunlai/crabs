package org.codefamily.crabs.jdbc.engine;

import org.codefamily.crabs.core.client.AdvancedClient;
import org.codefamily.crabs.jdbc.lang.Statement;
import org.codefamily.crabs.exception.SQL4ESException;

public abstract class StatementExecutor<TStatement extends Statement, TResult> {

    protected StatementExecutor(final Class<TStatement> statementClass,
                                final Class<TResult> resultClass) {
        if (statementClass == null) {
            throw new IllegalArgumentException("Argument[statementClass] is null.");
        }
        if (resultClass == null) {
            throw new IllegalArgumentException("Argument[resultClass] is null.");
        }
        this.statementClass = statementClass;
        this.resultClass = resultClass;
    }

    final Class<TStatement> statementClass;

    final Class<TResult> resultClass;

    StatementExecutor<?, ?> nextInSet;

    protected abstract TResult execute(AdvancedClient advancedClient,
                                       TStatement statement,
                                       ExecuteEnvironment environment,
                                       Object[] argumentValues) throws SQL4ESException;

}
