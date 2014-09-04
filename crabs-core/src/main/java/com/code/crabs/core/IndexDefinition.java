package com.code.crabs.core;

import static com.code.crabs.common.Constants.DEFAULT_INDEX_REPLICAS_NUM;
import static com.code.crabs.common.Constants.DEFAULT_INDEX_SHARDS_NUM;

/**
 * 定义elasticsearch中的Index结构
 *
 * @author zhuchunlai
 * @version $Id: IndexDefinition.java, v1.0 2014/07/30 15:36 $
 */
public final class IndexDefinition {

    private final Identifier identifier;

    private final int shardsNum;

    private final int replicasNum;

    /**
     * 构造方法，shard数量和replicas数量均采用默认值，具体见{@link com.code.crabs.common.Constants#DEFAULT_INDEX_SHARDS_NUM}和
     * {@link com.code.crabs.common.Constants#DEFAULT_INDEX_REPLICAS_NUM}
     *
     * @param identifier index名称标识
     */
    public IndexDefinition(final Identifier identifier) {
        this(identifier, DEFAULT_INDEX_SHARDS_NUM, DEFAULT_INDEX_REPLICAS_NUM);
    }

    /**
     * 构造方法
     *
     * @param identifier  index名称标识
     * @param shardsNum   shard的数量，具体见elasticsearch中的概念
     * @param replicasNum replicas的数量，具体见elasticsearch中的概念
     */
    public IndexDefinition(final Identifier identifier, final int shardsNum, final int replicasNum) {
        if (identifier == null) {
            throw new IllegalArgumentException("Argument[identifier] is required.");
        }
        if (shardsNum < 1) {
            throw new IllegalArgumentException("Value of argument[shardsNum] is must greater than 1.");
        }
        if (replicasNum < 0) {
            throw new IllegalArgumentException("Value of argument[replicasNum] is must greater than 0.");
        }
        this.identifier = identifier;
        this.shardsNum = shardsNum;
        this.replicasNum = replicasNum;
    }

    public final Identifier getIdentifier() {
        return this.identifier;
    }

    public final int getShardsNum() {
        return this.shardsNum;
    }

    public final int getReplicasNum() {
        return this.replicasNum;
    }

    @Override
    public final boolean equals(final Object obj) {
        return obj instanceof IndexDefinition
                && this.identifier.equals(((IndexDefinition) obj).identifier);
    }

    @Override
    public final int hashCode() {
        return this.identifier.hashCode();
    }

}
