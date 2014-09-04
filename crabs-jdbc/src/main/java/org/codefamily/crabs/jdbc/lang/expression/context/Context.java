package org.codefamily.crabs.jdbc.lang.expression.context;

import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.jdbc.lang.expression.Argument;
import org.codefamily.crabs.jdbc.lang.expression.Reference;

import java.sql.Timestamp;

public abstract class Context {

    public abstract Timestamp getStartTime();

    public abstract Object getReferenceValue(Reference reference);

    public abstract DataType getArgumentValueType(Argument argument);

    public abstract Object getArgumentValue(Argument argument);

}
