package apoc;

import apoc.export.csv.ExportCSV;
import apoc.export.csv.ImportCsv;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.export.json.ImportJson;
import apoc.export.parquet.ExportParquet;
import apoc.export.parquet.ImportParquet;
import apoc.load.LoadCsv;
import apoc.load.LoadJson;
import apoc.load.LoadParquet;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Map;
import java.util.stream.IntStream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testCallCount;
import static org.neo4j.configuration.SettingValueParsers.BYTES;


public class PerformancesTest {
    private static final File directory = new File("target/import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db; {
        String value = "26G";
        db = new ImpermanentDbmsRule()
                .withSetting(GraphDatabaseSettings.memory_tracking, true)
                .withSetting(BootloaderSettings.initial_heap_size, BYTES.parse(value))
                .withSetting(BootloaderSettings.max_heap_size, BYTES.parse(value))
                .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());
    }
    

    @Before
    public void setUp() throws Exception {
        testCallCount(db, "MATCH (n) RETURN n", 0);
        
        // register all the export/import/load procedures
        TestUtil.registerProcedure(db, Meta.class,
                ImportParquet.class, ExportParquet.class, LoadParquet.class,
                ExportCSV.class, LoadCsv.class, ImportCsv.class,
                ExportGraphML.class,
                ImportJson.class, ExportJson.class, LoadJson.class);
        
        // APOC config with `apoc.import.file.enabled=true`, `apoc.export.file.enabled=true`
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        
        // create constraint required by apoc.export.json.* procedures
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:Start) REQUIRE n.neo4jImportId IS UNIQUE;");
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:End) REQUIRE n.neo4jImportId IS UNIQUE;");
        
        IntStream.range(0, 50)
                .forEach(__-> db.executeTransactionally("UNWIND range(0, 19999) as id WITH id " +
                                                        "CREATE (:Start {idStart: id})-[:REL {idRel: id}]->(:End {idEnd: id})")
                );
    }

    @Test
    public void testPerformanceImportAndExportCsv() {
        exportCsv();
        importCsv();
    }

    @Test
    public void testPerformanceLoadAndExportCsv() {
        exportCsvForLoad();
        loadCsv();
    }

    @Test
    public void testPerformance1ImportAndExportParquet() {
        exportParquet();
        importParquet();
    }

    @Test
    public void testPerformance1LoadAndExportParquet() {
        exportParquet();
        loadParquet();
    }

    @Test
    public void testPerformanceImportAndExportJson() {
        exportJson();
        importJson();
    }

    @Test
    public void testPerformanceLoadAndExportJson() {
        exportJson();
        loadJson();
    }

    @Test
    public void testPerformanceImportAndExportGraphMl() {
        exportGraphMl();
        importGraphMl();
    }
    

    private void exportJson() {
        testPerformanceCommon("CALL apoc.export.json.all('test.json')", "endExportJson = ");
    }

    private void exportGraphMl() {
        testPerformanceCommon("CALL apoc.export.graphml.all('test.graphml', {})", "endExportGraphMlJson = ");
    }

    private void exportParquet() {
        testPerformanceCommon("CALL apoc.export.parquet.all('test.parquet')", "endExportParquet = ");
    }

    private void importJson() {
        testPerformanceCommon("CALL apoc.import.json('test.json')", "endImportJson = ");
    }

    private void importGraphMl() {
        testPerformanceCommon("CALL apoc.import.graphml('test.graphml', {})", "endImportGraphmlJson = ");
    }

    private void exportCsv() {
        testPerformanceCommon("CALL apoc.export.csv.all('test.csv', {bulkImport: true})", "endExportCsv = ");
    }

    private void exportCsvForLoad() {
        testPerformanceCommon("CALL apoc.export.csv.all('test.csv', {})", "endExportForLoadCsv = ");
    }
    
    private void loadCsv() {
        testPerformanceCommon("CALL apoc.load.csv('test.csv') YIELD map", "endLoadCsv = ");
    }
    
    private void loadJson() {
        testPerformanceCommon("CALL apoc.load.json('test.json') YIELD value", "endLoadJson = ");
    }

    private void importCsv() {
        testPerformanceCommon("CALL apoc.import.csv([{fileName: 'test.nodes.Start.csv', labels: ['Start']}," +
                              "{fileName: 'test.nodes.End.csv', labels: ['End']}], [{fileName: 'test.relationships.REL.csv', type: 'REL'}], {}) ", "endImportCsv = ");
    }

    private void importParquet() {
        testPerformanceCommon("CALL apoc.import.parquet('test.parquet')", "endImportParquet = ");
    }

    private void loadParquet() {
        testPerformanceCommon("CALL apoc.load.parquet('test.parquet')", "endLoadParquet = ");
    }

    private void testPerformanceCommon(String call, String printTime) {
        long start = System.currentTimeMillis();
        // execute and consume the query
        db.executeTransactionally(call, Map.of(), Result::resultAsString);
        long end = System.currentTimeMillis() - start;
        System.out.println(printTime + end);
    }

}
