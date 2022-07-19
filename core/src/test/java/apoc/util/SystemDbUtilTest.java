package apoc.util;

import apoc.ApocConfig;
import org.junit.Before;
import org.junit.Test;

import static apoc.ApocConfig.apocConfig;
import static apoc.SystemLabels.ApocTrigger;
import static apoc.SystemLabels.ApocUuid;
import static apoc.SystemLabels.ApocCypherProcedures;
import static apoc.SystemLabels.DataVirtualizationCatalog;
import static apoc.util.SystemDbUtil.KEY_THIS_DB;
import static apoc.util.SystemDbUtil.isCurrentDb;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class SystemDbUtilTest {
    
    @Before
    public void before() {
        new ApocConfig();
    }

    @Test
    public void testIsCurrentDb() {
        final String anotherDb = "another";
        final String notSpecifiedDb = "notSpecifiedDb";
        apocConfig().setProperty(KEY_THIS_DB, true);
        apocConfig().setProperty(KEY_THIS_DB + "." + DEFAULT_DATABASE_NAME, false);
        apocConfig().setProperty(KEY_THIS_DB + "." + DEFAULT_DATABASE_NAME + "." + DataVirtualizationCatalog.getFeatureName(), true);
        
        apocConfig().setProperty(KEY_THIS_DB + "." + anotherDb, true);
        apocConfig().setProperty(KEY_THIS_DB + "." + anotherDb + "." + DataVirtualizationCatalog.getFeatureName(), false);
        apocConfig().setProperty(KEY_THIS_DB + "." + anotherDb + "." + ApocTrigger.getFeatureName(), true);

        assertTrue(isCurrentDb(DEFAULT_DATABASE_NAME, DataVirtualizationCatalog.getFeatureName()));
        assertFalse(isCurrentDb(DEFAULT_DATABASE_NAME, ApocUuid.getFeatureName()));
        assertFalse(isCurrentDb(DEFAULT_DATABASE_NAME, ApocTrigger.getFeatureName()));
        assertFalse(isCurrentDb(DEFAULT_DATABASE_NAME, ApocCypherProcedures.getFeatureName()));

        assertFalse(isCurrentDb(anotherDb, DataVirtualizationCatalog.getFeatureName()));
        assertTrue(isCurrentDb(anotherDb, ApocUuid.getFeatureName()));
        assertTrue(isCurrentDb(anotherDb, ApocTrigger.getFeatureName()));
        assertTrue(isCurrentDb(anotherDb, ApocCypherProcedures.getFeatureName()));

        assertTrue(isCurrentDb(notSpecifiedDb, DataVirtualizationCatalog.getFeatureName()));
        assertTrue(isCurrentDb(notSpecifiedDb, ApocUuid.getFeatureName()));
        assertTrue(isCurrentDb(notSpecifiedDb, ApocTrigger.getFeatureName()));
        assertTrue(isCurrentDb(notSpecifiedDb, ApocCypherProcedures.getFeatureName()));
    }
    
    @Test
    public void testIsCurrentDbSysDb() {
        apocConfig().setProperty(KEY_THIS_DB, true);
        
        assertFalse(isCurrentDb(SYSTEM_DATABASE_NAME, DataVirtualizationCatalog.getFeatureName()));
        assertFalse(isCurrentDb(SYSTEM_DATABASE_NAME, ApocTrigger.getFeatureName()));
    }
}
