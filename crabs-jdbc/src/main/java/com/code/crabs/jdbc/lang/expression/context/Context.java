package com.code.crabs.jdbc.lang.expression.context;

import com.code.crabs.core.DataType;
import com.code.crabs.jdbc.lang.expression.Argument;
import com.code.crabs.jdbc.lang.expression.Reference;

import java.sql.Timestamp;

public abstract class Context {

    public abstract Timestamp getStartTime();

    public abstract Object getReferenceValue(Reference reference);

    public abstract DataType getArgumentValueType(Argument argument);

    public abstract Object getArgumentValue(Argument argument);

}
