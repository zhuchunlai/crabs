package com.code.crabs.jdbc;

import com.code.crabs.core.client.AdvancedClient.ElasticsearchAddress;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import java.util.Hashtable;

public final class DataSourceFactory implements ObjectFactory {

    private final static String DATA_SOURCE_CLASS_NAME = DataSource.class
            .getName();

    public DataSourceFactory() {
        // to do nothing.
    }

    @Override
    public final Object getObjectInstance(final Object object,
                                          final Name name,
                                          final Context context,
                                          final Hashtable<?, ?> environment) throws Exception {
        final Reference reference = (Reference) object;
        final String className = reference.getClassName();
        if ((className != null) && className.equals(DATA_SOURCE_CLASS_NAME)) {
            final String serverAddresses = getReferenceValue(reference, "serverAddresses");
            final String databaseName = getReferenceValue(reference, "databaseName");
            final String userName = getReferenceValue(reference, "user");
            final String password = getReferenceValue(reference, "password");

            return new DataSource(
                    ElasticsearchAddress.toElasticsearchAddresses(serverAddresses),
                    databaseName,
                    userName,
                    password
            );
        } else {
            return null;
        }
    }

    private static String getReferenceValue(final Reference reference,
                                            final String referenceName) {
        final RefAddr refAddr = reference.get(referenceName);
        return refAddr != null ? (String) refAddr.getContent() : null;
    }

}
