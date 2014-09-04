package com.code.crabs.jdbc.engine;

import com.code.crabs.core.client.AdvancedClient;
import com.code.crabs.jdbc.lang.Statement;
import com.code.crabs.exception.crabsException;

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
                                       Object[] argumentValues) throws crabsException;

}
