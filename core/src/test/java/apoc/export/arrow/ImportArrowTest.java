package apoc.export.arrow;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URL;
import java.util.Set;

import static apoc.export.arrow.ArrowStreamTest.assertResults;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.NODE_COUNT;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ImportArrowTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, ImportArrow.class);
    }

    @Test
    public void testBasicImport() throws Throwable {
        testCall(db, NODE_COUNT, row -> assertEquals(0L, row.get("result")));

        URL url = ClassLoader.getSystemResource("nodes_all.arrow");
        URL url2 = ClassLoader.getSystemResource("edges_all.arrow");
        String query = "CALL apoc.import.arrow($url, $url2, {})";

        testCall(db, query, map("url",url.getPath(), "url2",url2.getPath()),
                (row) -> assertResults(row, "file")
        );

        testResult(db, "MATCH (n) RETURN n",
                (r) -> {
                    assertTrue(r.hasNext());
                    Node node1 = (Node) r.next().get("n");
                    assertEquals("User", node1.getLabels().iterator().next().name());
                    final Set<String> expectedPropNode1 = Set.of("name", "age", "male", "kids", "born", "place.latitude", "place.longitude", "place.crs", "place.height");
                    assertEquals(expectedPropNode1, node1.getAllProperties().keySet());
                    Node node2 = (Node) r.next().get("n");
                    assertEquals("User", node2.getLabels().iterator().next().name());
                    assertEquals(Set.of("name", "age"), node2.getAllProperties().keySet());
                    Node node3 = (Node) r.next().get("n");
                    assertEquals("User", node3.getLabels().iterator().next().name());
                    assertEquals(Set.of("age"), node3.getAllProperties().keySet());
                    Node node4 = (Node) r.next().get("n");
                    assertEquals("Another", node4.getLabels().iterator().next().name());
                    assertEquals(Set.of("foo"), node4.getAllProperties().keySet());
                    assertFalse(r.hasNext());
                });

        testResult(db, "MATCH ()-[r]->() RETURN r",
                (r) -> {
                    assertTrue(r.hasNext());
                    Relationship rel = (Relationship) r.next().get("r");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(Set.of("since", "bffSince"), rel.getAllProperties().keySet());
                    assertFalse(r.hasNext());
        });
    }
}
