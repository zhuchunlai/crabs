package com.code.crabs.jdbc.internal;

import com.code.crabs.core.DataType;
import com.code.crabs.core.Identifier;
import com.code.crabs.exception.SQL4ESException;

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

    public abstract boolean next() throws SQL4ESException;

    @Override
    public abstract void close() throws IOException;

}
