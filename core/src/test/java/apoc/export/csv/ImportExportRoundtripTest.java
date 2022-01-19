package apoc.export.csv;

import org.junit.Test;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static apoc.export.csv.CsvLoaderConstants.TYPE_ATTR;
import static apoc.export.csv.ExportCsvTest.assertResults;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;

public class ImportExportRoundtripTest extends ExportCsvUseTypeTest {
    
    // entities props + 5(:ID, :LABELS, :START_ID, :END_ID and :TYPE)
    private static final long EXPECTED_EXPORT_PROPS = EXPECTED_PROPS + 5;

    @Test
    public void testRoundtripCsv() {
        final String anotherRel = "ANOTHER_REL";
        final String relType = "REL_TYPE";
        final String superNode = "SuperNode";
        final String superNodeFooBar = "SuperNode.Foo.Bar";
        
        String ext = ".csv";
        String fileName = "roundtrip";

        // export file
        String queryExport = "CALL apoc.export.csv.all($fileName,{importToolArrays: true, quotes: 'always', bulkImport: true, separateHeader: false})";
        final String exportFileName = fileName + ext;
        testCall(db, queryExport, map("fileName", exportFileName),
                (r) -> assertResults(exportFileName, r, "database", EXPECTED_NODES, EXPECTED_RELS, EXPECTED_PROPS));

        // cancel current entities
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        // import file
        List<Map<String, String>> nodes = List.of(ANOTHER_NODE, superNode, superNodeFooBar).stream()
                .map(label -> Map.of("fileName", fileName + ".nodes." + label + ext))
                .collect(Collectors.toList());

        List<Map<String, String>> rels = List.of(relType, anotherRel).stream()
                .map(type -> Map.of("fileName", fileName + ".relationships." + type + ext))
                .collect(Collectors.toList());

        testCall(db, "CALL apoc.import.csv($nodes, $rels, {})",
                map("nodes", nodes, "rels", rels),
                (r) -> {
                    assertEquals("file", r.get("source"));
                    assertEquals(EXPECTED_NODES, r.get("nodes"));
                    assertEquals(EXPECTED_RELS, r.get("relationships"));
                    assertEquals(EXPECTED_EXPORT_PROPS, r.get("properties"));
                });

        // entities assertions
        try(Transaction tx = db.beginTx()) {
            final Node anotherNode = tx.findNodes(label(ANOTHER_NODE)).next();
            final Map<String, Object> expectedProps = new HashMap<>(Map.copyOf(ANOTHER_NODE_PROPS));
            expectedProps.putAll(Map.of("alpha", 12L, "zeta", 1.1D, "epsilon", 1L, "gamma", "A"));
            deepEqualsAssertions(anotherNode, expectedProps);

            tx.findNodes(label(superNode)).forEachRemaining(node -> {
                if (node.getProperty("foo", null) != null) {
                    final Set<String> actual = Iterables.stream(node.getLabels()).map(Label::name).collect(Collectors.toSet());
                    assertEquals(Set.of(superNode, "Foo", "Bar"), actual);
                } else {
                    deepEqualsAssertions(node, SUPER_NODE_PROPS);
                }
            });

            tx.getAllRelationships().forEach(rel -> {
                if (rel.getType().name().equals(relType)) {
                    deepEqualsAssertions(rel, REL_PROPS);
                } else {
                    assertEquals(anotherRel, rel.getType().name());
                    assertEquals(Map.of(TYPE_ATTR, anotherRel), rel.getAllProperties());
                }
            });
        }
    }

    private void deepEqualsAssertions(Entity entity, Map<String, Object> superNodeProps) {
        superNodeProps.forEach((k, v) -> {
            final Object property = entity.getProperty(k);
            assertTrue("Expected: " + v + ", actual: " + property, Objects.deepEquals(v, property));
        });
    }
}
