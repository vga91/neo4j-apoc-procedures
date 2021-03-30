package apoc.export.arrow;

import apoc.path.PathExplorer;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ImportArrowTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    static String nodes;
    static String edges;

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, ImportArrow.class);
    }

    @Test
    public void testBasicImport() throws Throwable {
        URL url = ClassLoader.getSystemResource("result_withQuery.arrow");
        URL url2 = ClassLoader.getSystemResource("result_withQuery.arrow");
        String query = "CALL apoc.import.arrow($url, $url2, {batchSize: 200})";
        // todo - cambiare
        TestUtil.testCall(db, query, map("url",url.toString().replace("file:", ""), "url2",url2.toString().replace("file:", "")),
                (row) -> {});

        TestUtil.testResult(db, "MATCH (n) RETURN n",
                (r) -> {
                    assertTrue(r.hasNext());
                    Node node = (Node) r.next().get("n");
//                    assertEquals("prova", node.getLabels().iterator().next().toString());
                    System.out.println("ImportArrowTest.testBasicImport");
        });

        TestUtil.testResult(db, "MATCH ()-[r]->() RETURN r",
                (r) -> {
                    assertTrue(r.hasNext());
                    Relationship rel = (Relationship) r.next().get("r");
//                    assertEquals("prova", node.getLabels().iterator().next().toString());
                    System.out.println("ImportArrowTest.testBasicImport");
        });
    }
}
