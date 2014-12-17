package org.codefamily.crabs.jdbc.internal;

import org.codefamily.crabs.core.DataType;
import org.codefamily.crabs.core.Identifier;
import org.codefamily.crabs.exception.CrabsException;

import java.io.Closeable;
import java.io.IOException;

public abstract class InternalResultSet implements Closeable {

    public static abstract class InternalMetaData {

        public abstract int getColumnCount();

        public abstract int getColumnIndex(Identifier columnIdentifier);

        public abstract Identifier getColumnIdentifier(int columnIndex);

        public abstract String getColumnLabel(int columnIndex);

        public abstract DataType getColumnValueType(int columnIndex);

        public abstract int getColumnDisplaySize(int columnIndex);

    }

    public abstract InternalMetaData getMetaData();

    public abstract Object getColumnValue(Identifier columnIdentifier);

    public abstract Object getColumnValue(int columnIndex);

    public abstract boolean next() throws CrabsException;

    @Override
    public abstract void close() throws IOException;

}
