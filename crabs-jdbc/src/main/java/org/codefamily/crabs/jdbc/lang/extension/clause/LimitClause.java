package org.codefamily.crabs.jdbc.lang.extension.clause;

import org.codefamily.crabs.common.util.ReadonlyList;
import org.codefamily.crabs.jdbc.lang.Clause;
import org.codefamily.crabs.jdbc.lang.Expression;
import org.codefamily.crabs.jdbc.lang.Keyword;
import org.codefamily.crabs.jdbc.lang.extension.ReservedKeyword;

public final class LimitClause extends Clause {

    public static final ReadonlyList<Keyword> PREFIX_KEYWORD_LIST
            = ReadonlyList.newInstance((Keyword) ReservedKeyword.LIMIT);

    public final Expression offset;

    public final Expression rowCount;

    public LimitClause(final Expression offset, final Expression rowCount) {
        super(PREFIX_KEYWORD_LIST);
        if (offset == null) {
            throw new IllegalArgumentException("Argument[offset] is null.");
        }
        if (rowCount == null) {
            throw new IllegalArgumentException("Argument[rowCount] is null.");
        }
        this.offset = offset;
        this.rowCount = rowCount;
    }

    @Override
    public final String toString() {
        return this.getPrefixKeywordsString() + " " + offset + ", " + rowCount;
    }

    @Override
    public final boolean equals(final Object object) {
        return object != null && (object instanceof LimitClause)
                && ((LimitClause) object).offset.equals(this.offset)
                && ((LimitClause) object).rowCount.equals(this.rowCount);
    }

}
