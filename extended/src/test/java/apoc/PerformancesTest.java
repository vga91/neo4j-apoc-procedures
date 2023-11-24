package apoc;

import apoc.export.arrow.ExportArrow;
import apoc.export.csv.*;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.*;
import apoc.export.parquet.*;
import apoc.load.*;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import org.junit.*;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.*;

import java.io.File;
import java.util.Map;
import java.util.stream.IntStream;

import static org.neo4j.configuration.SettingValueParsers.BYTES;

/**
 * NOTE: 
 * This file is supposed to be used withing APOC Extended,
 * that is, in this folder: https://github.com/neo4j-contrib/neo4j-apoc-procedures/tree/dev/extended/src/test/java/apoc
 */
public class PerformancesTest {
    private static final File directory = new File("target/import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db; {
        // set a new GraphDatabaseService, for each test, with high heap space
        String value = "26G";
        db = new ImpermanentDbmsRule()
                .withSetting(GraphDatabaseSettings.memory_tracking, true)
                .withSetting(BootloaderSettings.initial_heap_size, BYTES.parse(value))
                .withSetting(BootloaderSettings.max_heap_size, BYTES.parse(value))
                .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());
    }

    @Before
    public void setUp() throws Exception {
        // register all the export/import/load procedures involved
        TestUtil.registerProcedure(db, Meta.class,
                ImportParquet.class, ExportParquet.class, LoadParquet.class,
                ExportCSV.class, LoadCsv.class, ImportCsv.class,
                ExportGraphML.class,
                ExportArrow.class, LoadArrow.class,
                ImportJson.class, ExportJson.class, LoadJson.class);

        // APOC config with `apoc.import.file.enabled=true`, `apoc.export.file.enabled=true`
        ApocConfig.apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, true);
        ApocConfig.apocConfig().setProperty(ApocConfig.APOC_EXPORT_FILE_ENABLED, true);

        // create constraint required by apoc.export.json.* procedures
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:Start) REQUIRE n.neo4jImportId IS UNIQUE;");
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:End) REQUIRE n.neo4jImportId IS UNIQUE;");

        IntStream.range(0, 50)
                .forEach(__-> db.executeTransactionally("UNWIND range(0, 19999) as id WITH id " +
                                                        "CREATE (:Start {idStart: id})-[:REL {idRel: id}]->(:End {idEnd: id})")
                );
    }

//    @Test
//    public void testPerformanceImportAndExportCsv() {
//        testPerformanceCommon("CALL apoc.export.csv.all('test.csv', {bulkImport: true})");
//        testPerformanceCommon("CALL apoc.import.csv([{fileName: 'test.nodes.Start.csv', labels: ['Start']}," +
//                              "{fileName: 'test.nodes.End.csv', labels: ['End']}], [{fileName: 'test.relationships.REL.csv', type: 'REL'}], {}) ");
//    }
//
//    @Test
//    public void testPerformanceLoadAndExportCsv() {
//        testPerformanceCommon("CALL apoc.export.csv.all('test.csv', {})");
//        testPerformanceCommon("CALL apoc.load.csv('test.csv') YIELD map");
//    }

    @Test
    public void testPerformanceLoadAndExportArrow() {
        testPerformanceCommon("CALL apoc.export.arrow.all('test.arrow')");
        testPerformanceCommon("CALL apoc.load.arrow('test.arrow')");
    }

    @Test
    public void testPerformanceLoadImportAndExportParquet() {
        testPerformanceCommon("CALL apoc.export.parquet.all('test.parquet')");
        testPerformanceCommon("CALL apoc.import.parquet('test.parquet')");
        // load
        testPerformanceCommon("CALL apoc.load.parquet('test.parquet')");
    }

//    @Test
//    public void testPerformanceLoadImportAndExportJson() {
//        testPerformanceCommon("CALL apoc.export.json.all('test.json')");
//        testPerformanceCommon("CALL apoc.import.json('test.json')");
//        // load
//        testPerformanceCommon("CALL apoc.load.json('test.json') YIELD value");
//    }
//
//    @Test
//    public void testPerformanceImportAndExportGraphMl() {
//        testPerformanceCommon("CALL apoc.export.graphml.all('test.graphml', {})");
//        testPerformanceCommon("CALL apoc.import.graphml('test.graphml', {})");
//    }

    // execute and consume the query
    // get timestamp before and after the procedure and print it
    private void testPerformanceCommon(String call) {
        long start = System.currentTimeMillis();
        // execute
        db.executeTransactionally(call, Map.of(), Iterators::count);
        long end = System.currentTimeMillis() - start;

        // print the time occurred
        String printTime = "Time occurred for the `%s` procedure is:\n%s\n".formatted(call, end);
        System.out.println(printTime);
    }
}