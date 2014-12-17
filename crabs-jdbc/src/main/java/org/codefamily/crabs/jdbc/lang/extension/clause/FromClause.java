package org.codefamily.crabs.jdbc.lang.extension.clause;

import org.codefamily.crabs.util.ReadonlyList;
import org.codefamily.crabs.util.StringUtils;
import org.codefamily.crabs.core.Identifier;
import org.codefamily.crabs.jdbc.lang.Clause;
import org.codefamily.crabs.jdbc.lang.Keyword;
import org.codefamily.crabs.jdbc.lang.extension.ReservedKeyword;

public final class FromClause extends Clause {

    public static final ReadonlyList<Keyword> PREFIX_KEYWORD_LIST = ReadonlyList
            .newInstance((Keyword) ReservedKeyword.FROM);

    public FromClause(final TableDeclare... tableDeclares) {
        super(PREFIX_KEYWORD_LIST);
        if (tableDeclares == null) {
            throw new IllegalArgumentException("Argument[sourceSetDeclares] is null.");
        }
        if (tableDeclares.length < 1) {
            throw new IllegalArgumentException("Table declare must be more than one.");
        }
        for (int i = 0; i < tableDeclares.length; i++) {
            if (tableDeclares[i] == null) {
                throw new IllegalArgumentException("Argument [tableDeclares[" + i + "]] is null.");
            }
        }
        this.tableDeclareList = ReadonlyList.newInstance(tableDeclares.clone());
    }

    public final ReadonlyList<TableDeclare> tableDeclareList;

    @Override
    public final String toString() {
        final ReadonlyList<TableDeclare> tableDeclareList = this.tableDeclareList;
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.getPrefixKeywordsString());
        stringBuilder.append(' ');
        stringBuilder.append(tableDeclareList.get(0).toString());
        for (int i = 1, tableDeclareCount = tableDeclareList.size(); i < tableDeclareCount; i++) {
            stringBuilder.append(", ");
            stringBuilder.append(tableDeclareList.get(i).toString());
        }
        return stringBuilder.toString();
    }

    @Override
    public final boolean equals(final Object object) {
        if (object != null && object instanceof FromClause) {
            if (object == this) {
                return true;
            }
            final FromClause that = (FromClause) object;
            final ReadonlyList<TableDeclare> thisTableDeclareList = this.tableDeclareList;
            final ReadonlyList<TableDeclare> thatTableDeclareList = that.tableDeclareList;
            if (thisTableDeclareList.size() == thatTableDeclareList.size()) {
                for (int i = 0, thisTableDeclareCount = thisTableDeclareList.size();
                     i < thisTableDeclareCount; i++) {
                    if (!thisTableDeclareList.get(i).equals(thatTableDeclareList.get(i))) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public static final class SimpleTableDeclare extends TableDeclare {

        public final Identifier tableIdentifier;

        public SimpleTableDeclare(final String alias, final String tableName) {
            super(alias);
            if (StringUtils.isNullOrEmptyAfterTrim(tableName)) {
                throw new IllegalArgumentException("Argument[tableName] is null or empty.");
            }
            this.tableIdentifier = new Identifier(tableName);
        }

        public SimpleTableDeclare(final Identifier alias, final Identifier tableIdentifier) {
            super(alias);
            if (tableIdentifier == null) {
                throw new IllegalArgumentException("Argument[tableIdentifier] is null.");
            }
            this.tableIdentifier = tableIdentifier;
        }

        @Override
        public final boolean equals(final Object obj) {
            if (obj != null && obj instanceof SimpleTableDeclare) {
                final SimpleTableDeclare that = (SimpleTableDeclare) obj;
                return this.tableIdentifier.equals(that.tableIdentifier)
                        && (this.alias == null ? that.alias == null : this.alias.equals(that.alias));
            }
            return false;
        }

        private String toStringValue;

        @Override
        public final String toString() {
            if (this.toStringValue == null) {
                this.toStringValue = this.alias == null ? this.tableIdentifier.toString()
                        : this.tableIdentifier.toString() + " " + ReservedKeyword.AS + " "
                        + this.alias.toString();
            }
            return this.toStringValue;
        }
    }

}
