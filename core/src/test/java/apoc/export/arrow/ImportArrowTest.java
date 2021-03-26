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
//        nodes = Thread.currentThread().getContextClassLoader().getResource("nodes_withSchema.arrow").getPath();
//        nodes = Util.readResourceFile("nodes_withSchema.arrow");
//        edges = Util.readResourceFile("edges_withSchema.arrow");
//        String additionalLink = "match (p:Person{name:'Nora Ephron'}), (m:Movie{title:'When Harry Met Sally'}) create (p)-[:ACTED_IN]->(m)";
//        try (Transaction tx = db.beginTx()) {
//            tx.execute(movies);
//            tx.execute(additionalLink);
//            tx.commit();
//        }
    }

    @Test
    public void testBasicImport() throws Throwable {
        URL url = ClassLoader.getSystemResource("nodes_withAllMaybeQPQPQPQPQPQPPQQPPQPQPQPQ.arrow");
        URL url2 = ClassLoader.getSystemResource("edges_withAllMaybeQPQPQPQPQPQPPQQPPQPQPQPQ.arrow");
        String query = "CALL apoc.import.arrow($url, $url2, {batchSize: 200})";
        // todo - cambiare
        TestUtil.testCall(db, query, map("url",url.toString().replace("file:", ""), "url2",url2.toString().replace("file:", "")), (row) -> {
            List<String> expectedNames = new ArrayList<>(Arrays.asList("Robert Zemeckis", "Mike Nichols", "Ron Howard", "Frank Darabont", "Tom Tykwer", "Andy Wachowski", "Lana Wachowski", "Tom Hanks", "John Patrick Stanley", "Nora Ephron", "Penny Marshall", "Rob Reiner"));
        });

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
                    Relationship node = (Relationship) r.next().get("r");
//                    assertEquals("prova", node.getLabels().iterator().next().toString());
                    System.out.println("ImportArrowTest.testBasicImport");
        });
    }

//    @Test
//    public void testToJsonCollectNodes() throws Exception {
//        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place: point({x: 56.7, y: 12.78, z: 1.1, crs: 'wgs-84-3d'})}),(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:User),(e {pippo:'pluto'})");
//        String query = "MATCH (u) RETURN apoc.convert.toJson(collect(u)) as list";
//        TestUtil.testCall(db, query, (row) -> {
//            List<String> users = List.of("User");
//            List<Object> valueAsList = Util.fromJson((String) row.get("list"), List.class);
//            Assert.assertEquals(5, valueAsList.size());
//
//            Map<String, Object> nodeOne = (Map<String, Object>) valueAsList.get(0);
//            Map<String, Object> expectedMap = Map.of("name", "Adam",
//                    "age", 42L,
//                    "male", true,
//                    "kids", List.of("Sam", "Anna", "Grace"),
//                    "born", "2015-07-04T19:32:24",
//                    "place", Map.of(
//                            "latitude", 56.7, "longitude", 12.78, "crs", "wgs-84-3d", "height", 1.1
//                    ));
//            assertJsonNode(nodeOne, "0", users, expectedMap);
//
//            Map<String, Object> nodeTwo = (Map<String, Object>) valueAsList.get(1);
//            Map<String, Object> expectedMapTwo = Map.of(
//                    "name", "Jim",
//                    "age", 42L);
//            assertJsonNode(nodeTwo, "1", users, expectedMapTwo);
//
//            Map<String, Object> nodeThree = (Map<String, Object>) valueAsList.get(2);
//            Map<String, Object> expectedMapThree = Map.of(
//                    "age", 12L);
//            assertJsonNode(nodeThree, "2", users, expectedMapThree);
//
//            Map<String, Object> nodeFour= (Map<String, Object>) valueAsList.get(3);
//            assertJsonNode(nodeFour, "3", users, null);
//
//            Map<String, Object> nodeFive = (Map<String, Object>) valueAsList.get(4);
//            Map<String, Object> expectedMapFive = Map.of("pippo", "pluto");
//            assertJsonNode(nodeFive, "4", null, expectedMapFive);
//        });
//    }


    @Test
    public void shouldImportAllJsonWithPropertyMappings() throws Exception {
        // todo - stesso file ma con config

//        TestUtil.testCall(db, "CALL apoc.import.json($file, $config)",
//                map("file", filename, "config",
//                        map("nodePropertyMappings", map("User", map("place", "Point", "born", "LocalDateTime")),
//                                "relPropertyMappings", map("KNOWS", map("bffSince", "Duration"))), "unwindBatchSize", 1, "txBatchSize", 1),
//                (r) -> {
    }
}
