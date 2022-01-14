package apoc;

import apoc.export.csv.ExportCSV;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static apoc.kernel.KernelTestUtils.checkStatusDetails;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.getUrlFileName;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP;
import static org.neo4j.configuration.SettingValueParsers.BYTES;

@RunWith(Parameterized.class)
public class StatusDetailsCoreTest {

    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db;

    static {
        try {
            db = new ImpermanentDbmsRule()
                    .withSetting(ApocSettings.apoc_import_file_enabled, true)
                    .withSetting(ApocSettings.apoc_export_file_enabled, true)
                    .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        db.executeTransactionally("UNWIND range(1,99999) AS x CREATE (:Status:Iterate)");
//        TestUtil.registerProcedure(db, LoadJson.class, Xml.class, ExportCSV.class, ExportJson.class, ExportGraphML.class, ExportCypher.class, LoadCsv);
    }
    
    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Parameterized.Parameters
    public static Collection<String[]> data() {
        return Arrays.asList(new String[][]{
                { "status.graphml" , 
                        "MATCH (n:Status:Iterate) WITH collect(n) as nodes CALL apoc.export.graphml.data(nodes, [], $file, {}) yield data RETURN 1", 
                        "CALL apoc.import.graphml($file,{readLabels:true})", 
                        null }
//                {100,3}
//                {100,4},
//                {100,5},
//                {10,7},
//                {1000,2}
        });
    }

    @Parameterized.Parameter(0)
    public String file;

    @Parameterized.Parameter(1)
    public String exportQuery;

    @Parameterized.Parameter(2)
    public String importQuery;

    @Parameterized.Parameter(3)
    public String loadQuery;

    @Test
    public void testExportGraphML() {
//        final String file = ClassLoader.getSystemResource("largeFile.graphml").toString();
        
//        TestUtil.testCall(db, "MATCH (n:Status:Iterate) WITH collect(n) as nodes CALL apoc.export.graphml.data(nodes, [], 'status.graphml', {}) yield data RETURN 1", r -> {});
//        db.executeTransactionally("MATCH (n:Status:Iterate) WITH collect(n) as nodes CALL apoc.export.graphml.data(nodes, [], 'status.graphml', {}) yield data RETURN 1", Map.of(), Result::resultAsString);
                
                
//        db.executeTransactionally("UNWIND range(1,99999) AS x CREATE (:Status:Iterate)");
//        db.executeTransactionally("UNWIND range(1,99999) AS x CREATE (:Status:Iterate)");
//        db.executeTransactionally("UNWIND range(1,99999) AS x CREATE (:Status:Iterate)");
//        db.executeTransactionally("UNWIND range(1,99999) AS x CREATE (:Status:Iterate)");
//        db.executeTransactionally("UNWIND range(1,99999) AS x CREATE (:Status:Iterate)");
//        db.executeTransactionally("UNWIND range(1,99999) AS x CREATE (:Status:Iterate)");
//        db.executeTransactionally("UNWIND range(1,99999) AS x CREATE (:Status:Iterate)");
//        db.executeTransactionally("UNWIND range(1,99999) AS x CREATE (:Status:Iterate)");
//        final Runnable runnable = () -> db.executeTransactionally("MATCH (n:Status:Iterate) WITH collect(n) as nodes CALL apoc.export.graphml.data(nodes, [], 'status.graphml', {}) yield data RETURN 1", Map.of(), Result::resultAsString);
        
//        String file = "status.graphml";
        if (exportQuery != null) {
            checkStatusDetails(db, exportQuery, Map.of("file", file));
        }
//                "MATCH (n:Status:Iterate) WITH collect(n) as nodes CALL apoc.export.graphml.data(nodes, [], $file, {}) yield data RETURN 1", Map.of("file", file));

        System.out.println(importQuery);
//        System.out.println(loadQuery);

//        final Runnable runnableImport = () -> db.executeTransactionally("MATCH (n:Status:Iterate) WITH collect(n) as nodes CALL apoc.export.graphml.data(nodes, [], 'status.graphml', {})");
        if (importQuery != null) {
            checkStatusDetails(db, importQuery, map("file", file));
        }
        
        if (loadQuery != null) {
            checkStatusDetails(db, loadQuery, map("file", file));
        }
    }
}
