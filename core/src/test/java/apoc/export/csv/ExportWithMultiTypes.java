package apoc.export.csv;

import apoc.util.Util;
import org.junit.Test;

import static apoc.export.csv.ExportCsvTest.assertResults;
import static apoc.export.csv.ExportCsvTest.readFile;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class ExportWithMultiTypes extends ExportCsvUseTypeTest {
    
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
                        "CALL apoc.export.csv.graph(graph, $file, $config) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("file", fileName, 
                        "config", map("useTypes", true, "quotes", "none", "importToolArrays", true)),
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

}
