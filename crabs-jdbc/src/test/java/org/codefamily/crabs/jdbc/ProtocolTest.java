package org.codefamily.crabs.jdbc;

import org.codefamily.crabs.core.client.AdvancedClient;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

public class ProtocolTest {

    @Test
    public void testParseURL() throws Exception {
        final String url = "jdbc:elasticsearch://10.12.117.30:9301/elasticsearch?index=my_csv_index&type=my_csv_type";
        final Protocol actual = Protocol.parseURL(url);
        final Properties properties = new Properties();
        properties.put("index", "my_csv_index");
        properties.put("type", "my_csv_type");

        final Protocol expected = new Protocol(
                new AdvancedClient.ElasticsearchAddress[]{
                        new AdvancedClient.ElasticsearchAddress("10.12.117.30", 9301)
                },
                "elasticsearch",
                properties
        );
        assertEquals(expected, actual);

    }
}