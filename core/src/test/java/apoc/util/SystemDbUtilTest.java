package apoc.util;

import org.junit.Test;

import static apoc.ApocConfig.apocConfig;
import static apoc.SystemLabels.ApocTrigger;
import static apoc.SystemLabels.DataVirtualizationCatalog;
import static apoc.util.SystemDbUtil.KEY_THIS_DB;
import static apoc.util.SystemDbUtil.isCurrentDb;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class SystemDbUtilTest {

    @Test
    public void testIsCurrentDb() {
        final String anotherDb = "another";
        
        apocConfig().setProperty(KEY_THIS_DB, false);
        apocConfig().setProperty(KEY_THIS_DB + "." + DEFAULT_DATABASE_NAME, false);
        apocConfig().setProperty(KEY_THIS_DB + "." + DEFAULT_DATABASE_NAME + "." + DataVirtualizationCatalog.getFeatureName(), true);
        apocConfig().setProperty(KEY_THIS_DB + "." + anotherDb, true);
        apocConfig().setProperty(KEY_THIS_DB + "." + DEFAULT_DATABASE_NAME + "." + DataVirtualizationCatalog.getFeatureName(), false);
        apocConfig().setProperty(KEY_THIS_DB + "." + DEFAULT_DATABASE_NAME + "." + ApocTrigger.getFeatureName(), true);
        
        assertFalse(SYSTEM_DATABASE_NAME, DataVirtualizationCatalog.getFeatureName())
        assertFalse(SYSTEM_DATABASE_NAME, ApocTrigger.getFeatureName())
        assertTrue(SYSTEM_DATABASE_NAME, "")
    }

    
    // todo
    @Test
    public void testIsCurrentDbSysDb() {
        apocConfig().setProperty(KEY_THIS_DB, true);
        
        assertFalse(isCurrentDb(SYSTEM_DATABASE_NAME, DataVirtualizationCatalog.getFeatureName()));
        assertFalse(isCurrentDb(SYSTEM_DATABASE_NAME, ApocTrigger.getFeatureName()));
    }
}
