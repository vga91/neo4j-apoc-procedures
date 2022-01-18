package apoc.export.csv;

import apoc.ApocSettings;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.export.csv.ExportCsvTest.assertResults;
import static apoc.export.csv.ExportCsvTest.readFile;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;


// Created to not affect ExportCsvTest results
public class ExportCsvUseTypeTest {

    private static final long EXPECTED_NODES = 3L;
    private static final long EXPECTED_RELS = 2L;
    private static final long EXPECTED_PROPS = 18L;
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, ExportCsvTest.directory.toPath().toAbsolutePath())
            .withSetting(ApocSettings.apoc_import_file_enabled, true)
            .withSetting(ApocSettings.apoc_export_file_enabled, true);


    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class, ImportCsv.class);
        db.executeTransactionally("CREATE (n:SuperNode { one: datetime('2018-05-10T10:30[Europe/Berlin]'), two: time('18:02:33'), three: localtime('17:58:30'), \n" +
                "four: localdatetime('2021-06-08'), five: date('2020'), six: duration({months: 5, days: 1.5}), seven : '2020'}) \n" +
                "WITH n CREATE (n)-[:REL_TYPE {rel: point({x: 56.7, y: 12.78, crs: 'cartesian'})}]->(m:AnotherNode), \n" +
                "(m)-[:ANOTHER_REL]->(:SuperNode:Foo:Bar {foo: 'bar'})");
        
        try(Transaction tx = db.beginTx()) {
            final Node node = tx.findNodes(label("AnotherNode")).next();
            // force property type
            node.setProperty("alpha", (short) 1);
            node.setProperty("beta", "qwerty".getBytes());
            node.setProperty("gamma", 'A');
            node.setProperty("epsilon", 1);
            node.setProperty("zeta", 1.1F);
            node.setProperty("eta", 1L);
            node.setProperty("theta", 10.1D);
            node.setProperty("iota", "bar");
            node.setProperty("kappa", new String[] {"un", "deux", "trois"});
            tx.commit();
        }
    }

    @Test
    public void testExportCsvAll() {
        String fileName = "manyTypes.csv";
        testCall(db, "CALL apoc.export.csv.all($file, {useTypes: true, quotes: 'none', importToolArrays: true})", map("file", fileName),
                (r) -> assertResults(fileName, r, "database", EXPECTED_NODES, EXPECTED_RELS, EXPECTED_PROPS));
        final String expected = Util.readResourceFile(fileName);
        assertEquals(expected, readFile(fileName));

        // -- streaming mode
        String statement = "CALL apoc.export.csv.all(null, {stream:true, useTypes: true, quotes: 'none', importToolArrays: true})";
        testCall(db, statement, (r) -> assertEquals(expected, r.get("data")));
    }

    @Test
    public void testExportCsvGraph() {
        String fileName = "manyTypes.csv";
        testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.csv.graph(graph, $file,{useTypes: true, quotes: 'none', importToolArrays: true}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", fileName),
                (r) -> assertResults(fileName, r, "graph", EXPECTED_NODES, EXPECTED_RELS, EXPECTED_PROPS));
        final String expected = Util.readResourceFile(fileName);
        assertEquals(expected, readFile(fileName));
    }

    @Test
    public void testExportCsvGraphWithoutImportToolArrays() {
        String fileName = "manyTypesWithArrayLegacy.csv";
        testCall(db, "CALL apoc.export.csv.all($file, {useTypes: true, quotes: 'none'})", map("file", fileName),
                (r) -> assertResults(fileName, r, "database", EXPECTED_NODES, EXPECTED_RELS, EXPECTED_PROPS));
        final String expected = Util.readResourceFile(fileName);
        assertEquals(expected, readFile(fileName));

        // -- streaming mode
        String statement = "CALL apoc.export.csv.all(null, {stream:true, useTypes: true, quotes: 'none'})";
        testCall(db, statement, (r) -> assertEquals(expected, r.get("data")));
    }
    
    @Test
    public void testRoundtripCsv() {
        String ext = ".csv";
        String fileName = "roundtrip";

        String queryExport = "CALL apoc.export.csv.all($fileName,{bulkImport: true, separateHeader: false})";
        final String exportFileName = fileName + ext;
        testCall(db, queryExport, map("fileName", exportFileName),
                (r) -> assertResults(exportFileName, r, "database", EXPECTED_NODES, EXPECTED_RELS, EXPECTED_PROPS));
        
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        List<Map<String, Object>> nodes = List.of("AnotherNode", "SuperNode", "SuperNode.Foo.Bar").stream()
                .map(label -> Map.of("fileName", fileName + ".nodes." + label + ext, "labels", Arrays.asList(label.split("\\."))))
                .collect(Collectors.toList());
        
        List<Map<String, String>> rels = List.of("REL_TYPE", "ANOTHER_REL").stream()
                .map(type -> Map.of("fileName", fileName + ".relationships." + type + ext, "labels", type))
                .collect(Collectors.toList());
// todo - mettere un config...  fileName + ".nodes" + label + ext
        
        testCall(db, "CALL apoc.import.csv($nodes, $rels, {})", 
                map("nodes", nodes, "rels", rels),
                (r) -> {
                    assertEquals("file", r.get("source"));
                    assertEquals(EXPECTED_NODES, r.get("nodes"));
                    assertEquals(EXPECTED_RELS, r.get("relationships"));
//                    assertEquals(EXPECTED_PROPS, r.get("properties"));
                });

        try(Transaction tx = db.beginTx()) {
            System.out.println("ExportCsvUseTypeTest.testRoundtripCsv");
//            tx.findNodes(label("SuperNode"), "one",)
        }
        
        System.out.println("ExportCsvUseTypeTest.testRoundtripCsv");

//
//        List.of("AnotherNode", "SuperNode").forEach(label -> {
//            testCall(db, "CALL apoc.import.csv($fileName)", map("fileName", fileName + ".nodes" + label + ext),
//                (r) -> {
//                    assertEquals("database", r.get("source"));
//                });   
//        });
//
//        testCall(db, "CALL apoc.import.csv($fileName)", map("fileName", fileName + ".relationships" + "REL_TYPE" + ext),
//                (r) -> assertEquals("database", r.get("source")));

//        String queryImport = "CALL apoc.import.csv.all($fileName,{bulkImport: true, separateHeader: false})";
//        testCall(db, queryImport, map("fileName", fileName),
//                (r) -> assertEquals("database", r.get("source")));
    }
}
