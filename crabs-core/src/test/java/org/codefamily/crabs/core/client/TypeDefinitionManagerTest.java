package org.codefamily.crabs.core.client;

import org.codefamily.crabs.common.util.ReadonlyList;
import org.codefamily.crabs.core.Identifier;
import org.codefamily.crabs.core.IndexDefinition;
import org.codefamily.crabs.core.TypeDefinition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.codefamily.crabs.Constants.PATTERN_YYYY_MM_DD_HH_MM_SS;

public class TypeDefinitionManagerTest {

    private AdvancedClient advancedClient;

    private TypeDefinitionManager typeDefinitionManager;

    private IndexDefinition indexDefinition;

    private static final String HOST = "127.0.0.1";
//    private static final String HOST = "192.168.213.50";

    @Before
    public void setUp() throws Exception {
        this.advancedClient = new AdvancedClient(
                new AdvancedClient.ElasticsearchAddress[]{
                        new AdvancedClient.ElasticsearchAddress(HOST, 9300)
                }
        );
        this.typeDefinitionManager = new TypeDefinitionManager(this.advancedClient);
        this.indexDefinition = new IndexDefinition(new Identifier("storm_log"), 5, 1);
        final IndexDefinitionManager indexDefinitionManager = new IndexDefinitionManager(this.advancedClient);
        if (!indexDefinitionManager.exists(this.indexDefinition.getIdentifier())) {
            indexDefinitionManager.createIndex(this.indexDefinition);
        }
    }

    @After
    public void tearDown() throws Exception {
        this.advancedClient.close();
    }

    @Test
    public void testCreateType() throws Exception {
        final TypeDefinition typeDefinition = new TypeDefinition(
                new IndexDefinition(new Identifier("storm_log")),
                new Identifier("BJHC_16779"),
                true,
                false
        );
        typeDefinition.defineStringField(new Identifier("_string"));
        typeDefinition.defineIntegerField(new Identifier("_integer")).asPrimaryField();
        typeDefinition.defineLongField(new Identifier("_long"));
        typeDefinition.defineFloatField(new Identifier("_float"));
        typeDefinition.defineDoubleField(new Identifier("_double"));
        typeDefinition.defineBooleanField(new Identifier("_boolean"));
        typeDefinition.defineDateField(new Identifier("_date"), PATTERN_YYYY_MM_DD_HH_MM_SS);
        typeDefinition.publish();
        if (this.typeDefinitionManager.exists(typeDefinition)) {
            this.typeDefinitionManager.dropType(typeDefinition);
        }
        this.typeDefinitionManager.createType(typeDefinition);
        assertTrue(this.typeDefinitionManager.exists(typeDefinition));
    }

    @Test
    public void testAlterType() throws Exception {

    }

    @Test
    public void testDropType() throws Exception {
        final TypeDefinition typeDefinition = new TypeDefinition(
                this.indexDefinition,
                new Identifier("BJHC_16779")
        );
        typeDefinition.defineStringField(new Identifier("_string"));
        typeDefinition.defineIntegerField(new Identifier("_integer")).asPrimaryField();
        typeDefinition.defineLongField(new Identifier("_long"));
        typeDefinition.defineFloatField(new Identifier("_float"));
        typeDefinition.defineDoubleField(new Identifier("_double"));
        typeDefinition.defineBooleanField(new Identifier("_boolean"));
        typeDefinition.defineDateField(new Identifier("_date"), PATTERN_YYYY_MM_DD_HH_MM_SS);
        typeDefinition.publish();
        if (!this.typeDefinitionManager.exists(typeDefinition)) {
            this.typeDefinitionManager.createType(typeDefinition);
        }
        this.typeDefinitionManager.dropType(typeDefinition);
        assertFalse(this.typeDefinitionManager.exists(typeDefinition));
    }

    @Test
    public void testGetTypeDefinitions() throws Exception {
        final TypeDefinition bjhc16779 = new TypeDefinition(this.indexDefinition, new Identifier("BJHC_16779"), true, false);
        bjhc16779.defineStringField(new Identifier("_string"));
        bjhc16779.defineIntegerField(new Identifier("_integer")).asPrimaryField();
        bjhc16779.defineLongField(new Identifier("_long"));
        bjhc16779.defineFloatField(new Identifier("_float"));
        bjhc16779.defineDoubleField(new Identifier("_double"));
        bjhc16779.defineBooleanField(new Identifier("_boolean"));
        bjhc16779.defineDateField(new Identifier("_date"), PATTERN_YYYY_MM_DD_HH_MM_SS);
        bjhc16779.publish();
        final ReadonlyList<TypeDefinition> expected = ReadonlyList.newInstance(bjhc16779);
        final ReadonlyList<TypeDefinition> actual = this.typeDefinitionManager.getTypeDefinitions(new Identifier("storm_log"));
        assertEquals(expected, actual);
    }

    @Test
    public void testGetTypeDefinition() throws Exception {
        final Identifier typeIdentifier = new Identifier("BJHC_16779");
        final TypeDefinition expected = new TypeDefinition(this.indexDefinition, typeIdentifier, true, false);
        expected.defineStringField(new Identifier("_string"));
        expected.defineIntegerField(new Identifier("_integer")).asPrimaryField();
        expected.defineLongField(new Identifier("_long"));
        expected.defineFloatField(new Identifier("_float"));
        expected.defineDoubleField(new Identifier("_double"));
        expected.defineBooleanField(new Identifier("_boolean"));
        expected.defineDateField(new Identifier("_date"), PATTERN_YYYY_MM_DD_HH_MM_SS);
        expected.publish();
        if (!this.typeDefinitionManager.exists(expected)) {
            this.typeDefinitionManager.createType(expected);
        }
        final TypeDefinition actual = this.typeDefinitionManager.getTypeDefinition(indexDefinition, typeIdentifier);
        assertEquals(expected, actual);
    }

    @Test
    public void testExists() throws Exception {

    }
}