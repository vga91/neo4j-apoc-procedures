package apoc.load;

import apoc.util.TestUtil;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.*;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.*;

public class LoadDirectoryTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        File importFolder = new File(temporaryFolder.getRoot() + "/importDir");
        DatabaseManagementService databaseManagementService = new TestDatabaseManagementServiceBuilder(importFolder).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

        TestUtil.registerProcedure(db, LoadDirectory.class);

        // create temp files and subfolder
        temporaryFolder.newFile("Foo.csv");
        temporaryFolder.newFile("Bar.csv");
        temporaryFolder.newFile("Baz.xls");
        temporaryFolder.newFolder("importDir/subfolder");
        temporaryFolder.newFile("importDir/TestCsv1.csv");
        temporaryFolder.newFile("importDir/TestCsv2.csv");
        temporaryFolder.newFile("importDir/TestCsv3.csv");
        temporaryFolder.newFile("importDir/TestXls1.xsl");
        temporaryFolder.newFile("importDir/TestJson1.json");
        temporaryFolder.newFile("importDir/subfolder/TestSubfolder.json");
        temporaryFolder.newFile("importDir/subfolder/TestSubfolder.csv");
    }

    @Test
    public void testWithSubfolder() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        // todo
        testResult(db, "CALL apoc.load.directory('*', 'subfolder', {recursive: false}) YIELD url RETURN url", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("url"));
                    assertTrue(rows.contains("subfolder/TestSubfolder.json"));
                    assertTrue(rows.contains("subfolder/TestSubfolder.csv"));
                    assertEquals(2, rows.size());
                }
        );
    }

    @Test
    public void testWithFileProtocol() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        File rootTempFolder = temporaryFolder.getRoot();
        String folderAsExternalUrl = "file://" + rootTempFolder;
        // todo
        testResult(db, "CALL apoc.load.directory('*', '" + folderAsExternalUrl + "', {recursive: false}) YIELD url RETURN url", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("url"));
                    assertTrue(rows.contains(rootTempFolder + "/Foo.csv"));
                    assertTrue(rows.contains(rootTempFolder + "/Bar.csv"));
                    assertTrue(rows.contains(rootTempFolder + "/Baz.xls"));
                    assertEquals(3, rows.size());
                }
        );
    }

    @Test
    public void testWithFilterAllRecursiveFalse() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        // todo
        testResult(db, "CALL apoc.load.directory('*', null, {recursive: false}) YIELD url RETURN url", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("url"));
                    assertTrue(rows.contains("TestCsv3.csv"));
                    assertTrue(rows.contains("TestJson1.json"));
                    assertEquals(5, rows.size());
                }
        );
    }

    @Test
    public void testWithFilterCsv() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        testResult(db, "CALL apoc.load.directory('*.csv', null, {}) YIELD url RETURN url", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("url"));
                    assertTrue(rows.contains("TestCsv1.csv"));
                    assertTrue(rows.contains("TestCsv2.csv"));
                    assertTrue(rows.contains("TestCsv3.csv"));
                    assertTrue(rows.contains("subfolder/TestSubfolder.csv"));
                    assertEquals(4, rows.size());
                }
        );
    }

}
