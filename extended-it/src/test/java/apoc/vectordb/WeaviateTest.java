package apoc.vectordb;

import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.weaviate.WeaviateContainer;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static apoc.vectordb.VectorDbTestUtil.assertBerlinVector;
import static apoc.vectordb.VectorDbTestUtil.assertLondonVector;
import static apoc.vectordb.VectorDbTestUtil.assertNodesCreated;
import static apoc.vectordb.VectorDbTestUtil.assertOtherNodesCreated;
import static apoc.vectordb.VectorDbTestUtil.assertRelsAndIndexesCreated;
import static apoc.vectordb.VectorDbTestUtil.dropAndDeleteAll;
import static apoc.vectordb.VectorDbTestUtil.vectorEntityAssertions;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;


/**
 * TODO:
 * i vettori non hanno dimensione impostabile. Prende il primo che trova.
 * 
 */
public class WeaviateTest {
    public static final List<String> FIELDS = List.of("city", "foo");
    
    /*
    TODO
        echo '{"query": "{Get{TestClass(nearVector: {vector: [0.1] } ){alfa\\n_additional {distance}}}}"}' | curl \
        -X POST \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer WsXomuFc9gEPTkDv1sEwnas7Um7r1wXtFZqs" \
        -d @- \
        https://test-apoc-sandbox-o66lj1p9.weaviate.network/v1/graphql

     */
    
    private static String API_KEY;
    private static String HOST;

    private static final WeaviateContainer weaviate = new WeaviateContainer("semitechnologies/weaviate:1.24.5");

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();
    
    private static final String id1 = "8ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308";
    private static final String id2 = "9ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308";

    @BeforeClass
    public static void setUp() throws Exception {
        weaviate.start();
        HOST = weaviate.getHttpHostAddress();// "localhost:" + weaviate.getMappedPort(8080);
        
//        API_KEY = checkEnvVar("WEAVIATE_KEY");
//        HOST = checkEnvVar("WEAVIATE_HOST");

        TestUtil.registerProcedure(db, Weaviate.class);

        testCall(db, "CALL apoc.vectordb.weaviate.createCollection($host, 'TestCollection', 'cosine', 4)",
                MapUtil.map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("TestCollection", value.get("class"));
                });

        testResult(db, """
                        CALL apoc.vectordb.weaviate.upsert($host, 'TestCollection',
                        [
                            {id: $id1, vector: [0.05, 0.61, 0.76, 0.74], metadata: {city: "Berlin", foo: "one"}},
                            {id: $id2, vector: [0.19, 0.81, 0.75, 0.11], metadata: {city: "London", foo: "two"}}
                        ])
                        """,
                MapUtil.map("host", HOST, "id1", id1, "id2", id2),
                r -> {
                    ResourceIterator<Map> values = r.columnAs("value");
                    assertEquals("TestCollection", values.next().get("class"));
                    assertEquals("TestCollection", values.next().get("class"));
                    assertFalse(values.hasNext());
                });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCallEmpty(db, "CALL apoc.vectordb.weaviate.deleteCollection($host, 'TestCollection')",
                MapUtil.map("host", HOST)
        );
    }

    @Before
    public void before() {
        dropAndDeleteAll(db);
    }


    @Test
    public void getEmbeddings() {
        testResult(db, "CALL apoc.vectordb.weaviate.get($host, 'TestCollection', [$id1])",
                Map.of("host", HOST, "id1", id1),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row, id1);
                    assertNotNull(row.get("vector"));
                });
    }

    @Test
    public void deleteVector() {
        testResult(db, """
                        CALL apoc.vectordb.weaviate.upsert($host, 'TestCollection',
                        [
                            {id: '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308', vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: "baz"}},
                            {id: '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309', vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: "baz"}}
                        ])
                        """,
                MapUtil.map("host", HOST),
                r -> {
                    ResourceIterator<Map> values = r.columnAs("value");
                    assertEquals("TestCollection", values.next().get("class"));
                    assertEquals("TestCollection", values.next().get("class"));
                    assertFalse(values.hasNext());
                });

        testCall(db, "CALL apoc.vectordb.weaviate.delete($host, 'TestCollection', " +
                     "['7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308', '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309']) ",
                Map.of("host", HOST),
                r -> {
                    List value = (List) r.get("value");
                    assertEquals(List.of("7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308", "7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309"), value);
                });
    }

    @Test
    public void getEmbedding() {
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD metadata, id, score, vector RETURN * ORDER BY id",
                Map.of("host", HOST, "conf", map("fields", FIELDS)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row, id1);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonVector(row, id2);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });
    }

    @Test
    public void getEmbeddingWithYield() {
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       "YIELD metadata, id RETURN * ORDER BY id",
                Map.of("host", HOST, "conf", map("fields", FIELDS)),
                r -> {
                    assertBerlinVector(r.next(), id1);
                    assertLondonVector(r.next(), id2);
                });
    }
// where: {operator: Equal, valueString: \"beta1\", path: [\"alfa\"]}
    @Test
    public void getEmbeddingWithFilter() {
        testResult(db, """
                        CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7],
                        '{operator: Equal, valueString: "London", path: ["city"]}',
                        5, $conf) YIELD metadata, id RETURN * ORDER BY id""",
                Map.of("host", HOST, "conf", map("fields", FIELDS)),
                r -> {
                    assertLondonVector(r.next(), id2);
                });
    }

    @Test
    public void getEmbeddingWithLimit() {
        testResult(db, """
                        CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 1, $conf) YIELD metadata, id RETURN * ORDER BY id""",
                Map.of("host", HOST, "conf", map("fields", FIELDS)),
                r -> {
                    assertBerlinVector(r.next(), id1);
                });
    }

    @Test
    public void getEmbeddingWithCreateIndex() {

        Map<String, Object> conf = Map.of("fields", FIELDS,
                MAPPING_KEY, Map.of("embeddingProp", "vect",
                "label", "Test",
                "prop", "myId",
                "id", "foo",
                "create", true));
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD metadata, id, score, vector RETURN * ORDER BY id",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row, id1);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonVector(row, id2);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db, true);

        testResult(db, "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                r -> vectorEntityAssertions(r, true));

        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD metadata, id, score, vector RETURN * ORDER BY id",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row, id1);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonVector(row, id2);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertOtherNodesCreated(db);
    }

    @Test
    public void getEmbeddingWithCreateIndexUsingExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = Map.of("fields", FIELDS,
                MAPPING_KEY, Map.of("embeddingProp", "vect",
                "label", "Test",
                "prop", "myId",
                "id", "foo"));
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD metadata, id, score, vector RETURN * ORDER BY id",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row, id1);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonVector(row, id2);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db, false);
    }

    @Test
    public void getEmbeddingWithCreateRelIndex() {

        db.executeTransactionally("CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");

        Map<String, Object> conf = Map.of("fields", FIELDS,
                MAPPING_KEY, Map.of("embeddingProp", "vect",
                "type", "TEST",
                "prop", "myId",
                "id", "foo"));
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD metadata, id, score, vector RETURN * ORDER BY id",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row, id1);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonVector(row, id2);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertRelsAndIndexesCreated(db);
    }

}
