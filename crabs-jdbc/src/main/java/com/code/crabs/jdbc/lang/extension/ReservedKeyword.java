package com.code.crabs.jdbc.lang.extension;

import com.code.crabs.jdbc.lang.Keyword;

public enum ReservedKeyword implements Keyword {
    //
    CREATE, DROP, ALTER, TABLE, SELECT, INSERT, UPDATE, DELETE, VALUES,
    //
    FROM, WHERE, GROUP, ORDER, NOT, AND, OR, BY, AS, IS, IN, BETWEEN,
    //
    LIKE, HAVING, FALSE, TRUE, NULL, MOD, CASE, WHEN, THEN, IF, LEFT, RIGHT,
    //
    OUTER, INNER, JOIN, END, LIMIT, NULLS, FIRST, LAST, ASC, DESC, DISTINCT,
    //
    TOP, SPLIT, ON, CONSTRAINT, UNIQUE, PRIMARY, KEY, EXISTS, IMPORT, EXPORT, TO;

    @Override
    public final String getName() {
        return this.name();
    }

}
