package com.code.crabs.core.client;

import com.code.crabs.common.util.ReadonlyList;
import com.code.crabs.core.Identifier;
import com.code.crabs.core.IndexDefinition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class IndexDefinitionManagerTest {

    private AdvancedClient advancedClient;

    private IndexDefinitionManager indexDefinitionManager;

    private static final String HOST = "127.0.0.1";
//    private static final String HOST = "192.168.213.50";

    @Before
    public final void setUp() {
        this.advancedClient = new AdvancedClient(
                new AdvancedClient.ElasticsearchAddress[]{
                        new AdvancedClient.ElasticsearchAddress(HOST, 9300)
                }
        );
        this.indexDefinitionManager = new IndexDefinitionManager(this.advancedClient);
    }

    @Test
    public final void testExists() throws Exception {
        final Identifier indexIdentifier = new Identifier("storm_log");
        boolean isExists = this.indexDefinitionManager.exists(indexIdentifier);
        if (isExists) {
            this.indexDefinitionManager.dropIndex(indexIdentifier);
            isExists = this.indexDefinitionManager.exists(indexIdentifier);
        }
        assertFalse(isExists);
    }

    @Test
    public final void testCreateIndex() throws Exception {
        final Identifier identifier = new Identifier("storm_log");
        final IndexDefinition indexDefinition = new IndexDefinition(identifier, 5, 1);
        if (this.indexDefinitionManager.exists(identifier)) {
            this.indexDefinitionManager.dropIndex(identifier);
        }
        this.indexDefinitionManager.createIndex(indexDefinition);
        assertTrue(this.indexDefinitionManager.exists(identifier));
    }

    @Test
    public final void testDropIndex() throws Exception {
        final Identifier identifier = new Identifier("storm_log");
        final IndexDefinition indexDefinition = new IndexDefinition(identifier, 5, 1);
        if (!this.indexDefinitionManager.exists(identifier)) {
            this.indexDefinitionManager.createIndex(indexDefinition);
        }
        this.indexDefinitionManager.dropIndex(identifier);
        assertFalse(this.indexDefinitionManager.exists(identifier));
    }

    @Test
    public final void testGetAllIndices() throws Exception {
        final ReadonlyList<IndexDefinition> expected = ReadonlyList.newInstance(
                new ArrayList<IndexDefinition>() {{
                    add(new IndexDefinition(new Identifier("test"), 1, 1));
                    add(new IndexDefinition(new Identifier("storm_log"), 5, 1));
                }}
        );
        for (IndexDefinition indexDefinition : expected) {
            if (!this.indexDefinitionManager.exists(indexDefinition.getIdentifier())) {
                this.indexDefinitionManager.createIndex(indexDefinition);
            }
        }
        final ReadonlyList<IndexDefinition> actual = this.indexDefinitionManager.getAllIndices();
        assertEquals(expected, actual);
    }

    @Test
    public final void testGetIndex() throws Exception {
        final Identifier indexIdentifier = new Identifier("storm_log");
        if (!this.indexDefinitionManager.exists(indexIdentifier)) {
            this.indexDefinitionManager.createIndex(new IndexDefinition(indexIdentifier));
        }
        this.indexDefinitionManager.getIndexDefinition(indexIdentifier);
    }

    @After
    public final void tearDown() throws IOException {
        this.advancedClient.close();
    }
}