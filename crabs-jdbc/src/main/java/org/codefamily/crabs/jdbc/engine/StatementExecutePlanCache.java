package org.codefamily.crabs.jdbc.engine;

import org.codefamily.crabs.jdbc.lang.Statement;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.HashMap;

final class StatementExecutePlanCache extends Thread {

    StatementExecutePlanCache() {
        this.setName("Statement execute plan cache cleaner");
        this.setDaemon(true);
        this.referenceQueue = new ReferenceQueue<StatementExecutePlan>();
        this.statementExecutePlanCache = new HashMap<Statement, StatementExecutePlanReference>();
    }

    private final ReferenceQueue<StatementExecutePlan> referenceQueue;

    private final HashMap<Statement, StatementExecutePlanReference> statementExecutePlanCache;

    @Override
    public final void run() {
        StatementExecutePlanReference selectExecutePlanReference;
        while ((selectExecutePlanReference = (StatementExecutePlanReference) this.referenceQueue.poll()) != null) {
            synchronized (this.statementExecutePlanCache) {
                this.statementExecutePlanCache.remove(selectExecutePlanReference.cacheKey);
            }
        }
    }

    final <TStatementExecutePlan extends StatementExecutePlan> TStatementExecutePlan getStatementExecutePlan(
            final Statement cacheKey,
            final Class<TStatementExecutePlan> planClass) {
        final StatementExecutePlanReference reference = this.statementExecutePlanCache.get(cacheKey);
        if (reference != null) {
            final StatementExecutePlan statementExecutePlan = reference.get();
            if (statementExecutePlan != null) {
                return planClass.cast(statementExecutePlan);
            }
        }
        return null;
    }

    final void putStatementExecutePlan(final Statement cacheKey,
                                       final StatementExecutePlan statementExecutePlan) {
        synchronized (this.statementExecutePlanCache) {
            this.statementExecutePlanCache.put(
                    cacheKey,
                    new StatementExecutePlanReference(cacheKey, statementExecutePlan, this.referenceQueue)
            );
        }
    }

    private static final class StatementExecutePlanReference extends SoftReference<StatementExecutePlan> {

        StatementExecutePlanReference(final Statement cacheKey,
                                      StatementExecutePlan referent,
                                      ReferenceQueue<? super StatementExecutePlan> referenceQueue) {
            super(referent, referenceQueue);
            this.cacheKey = cacheKey;
        }

        final Statement cacheKey;

    }

}
