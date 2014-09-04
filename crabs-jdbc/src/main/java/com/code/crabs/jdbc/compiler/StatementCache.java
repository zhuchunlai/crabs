package com.code.crabs.jdbc.compiler;

import com.code.crabs.jdbc.lang.Statement;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.HashMap;

/**
 * SQL语句缓存
 *
 * @author zhuchunlai
 * @version $Id: StatementCache.java, v1.0 2013/08/26 10:06 $
 */
public final class StatementCache {

    public StatementCache() {
        this.statementMap = new HashMap<String, StatementReference>();
        this.referenceQueue = new ReferenceQueue<Statement>();
        this.cleaner = new Cleaner();
    }

    private final HashMap<String, StatementReference> statementMap;

    private final ReferenceQueue<Statement> referenceQueue;

    private final Cleaner cleaner;

    public final Statement getStatement(final String statementString) {
        if (statementString == null) {
            throw new IllegalArgumentException("Argument [statementString] is null.");
        }
        final StatementReference reference = this.statementMap.get(statementString);
        return reference == null ? null : reference.get();
    }

    public final void putStatement(final String statementString,
                                   final Statement statement) {
        if (statementString == null) {
            throw new IllegalArgumentException("Argument [statementString] is null.");
        }
        if (statement == null) {
            throw new IllegalArgumentException("Argument [statement] is null.");
        }
        if (!this.statementMap.containsKey(statementString)) {
            synchronized (this.statementMap) {
                if (!this.statementMap.containsKey(statementString)) {
                    this.statementMap.put(
                            statementString,
                            new StatementReference(
                                    statementString,
                                    statement,
                                    this.referenceQueue
                            )
                    );
                }
            }
        }
    }

    public final void start() {
        this.cleaner.start();
    }

    public final void stop() {
        this.cleaner.setRunSwitch(false);
        this.cleaner.interrupt();
    }

    private final class Cleaner extends Thread {

        Cleaner() {
            super("Statement cache cleaner");
            this.setDaemon(true);
            this.runSwitch = true;
        }

        private volatile boolean runSwitch;

        final void setRunSwitch(final boolean runSwitch) {
            this.runSwitch = runSwitch;
        }

        @Override
        public final void run() {
            final HashMap<String, StatementReference> statementMap = StatementCache.this.statementMap;
            final ReferenceQueue<Statement> referenceQueue = StatementCache.this.referenceQueue;
            StatementReference statementReference;
            while (this.runSwitch
                    && (statementReference = (StatementReference) referenceQueue.poll()) != null) {
                synchronized (statementMap) {
                    statementMap.remove(statementReference.cacheKey);
                }
            }
        }
    }

    private static final class StatementReference extends SoftReference<Statement> {

        StatementReference(final String cacheKey, final Statement statement,
                           final ReferenceQueue<? super Statement> referenceQueue) {
            super(statement, referenceQueue);
            this.cacheKey = cacheKey;
        }

        final String cacheKey;

    }

}
