package apoc.vectordb;

import apoc.util.TestUtil;
import apoc.util.collection.Iterables;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.qdrant.QdrantContainer;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

// todo - duplicate and do a QdrantDbTest with custom procs..
public class QdrantDbTest {
/*
todo - create collection procs with 
                                name collection 
                              "size": 4,
                              "distance": "Cosine" 
 */
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static QdrantContainer qdrant = new QdrantContainer("qdrant/qdrant:v1.7.4");
    public static String HOST;

    @BeforeClass
    public static void setUp() throws Exception {
        qdrant.start();

        HOST = "localhost:" + qdrant.getMappedPort(6333);
        TestUtil.registerProcedure(db, VectorDb.class, Qdrant.class);

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);

        testCall(db, """
                        CALL apoc.vectordb.custom({
                        endpoint: $endpoint,
                        body: {
                            vectors: {
                              size: 4,
                              distance: "Cosine"
                            }
                        }, method: 'PUT'})""", 
                Map.of("endpoint", "http://localhost:" + qdrant.getMappedPort(6333) + "/collections/test_collection"),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });

        testCall(db, """
                        CALL apoc.vectordb.custom({
                        endpoint: $endpoint,
                        body: {
                              points: [
                                {
                                  id: 1,
                                  vector: [0.05, 0.61, 0.76, 0.74],
                                  payload: {city: "Berlin", foo: "one"}
                                },
                                {
                                  id: 2,
                                  vector: [0.19, 0.81, 0.75, 0.11],
                                  payload: {city: "London", foo: "two"}
                                }
                            ]
                        }, method: 'PUT'})""", Map.of("endpoint", "http://localhost:" + qdrant.getMappedPort(6333) + "/collections/test_collection/points"),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });

    }
    
    @Before
    public void after() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraints().forEach(ConstraintDefinition::drop);
            tx.schema().getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        }
    }

    @Test
    public void getEmbeddings() {
        testResult(db, "CALL apoc.vectordb.qdrant.get($host, 'test_collection', [1]) ",
                Map.of("host", HOST),
                r -> {
                    System.out.println("r = " + r.next());
                });
    }

    @Test
    public void getEmbedding() {
//        String filter = System.getenv("PINECONE_FILTER");
//        Assume.assumeNotNull("No PINECONE_FILTER environment configured", host);
// todo ->   nResults: 10, ovvero limit, come parametro opzionale
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5)",
                Map.of("host", HOST, /*"filter", filter, */"conf", emptyMap()),
                r -> {
                    System.out.println("r = " + r.next());
                    System.out.println("r = " + r.next());
                });
    }
    
    @Test
    public void getEmbeddingWithYield() {
//        String filter = System.getenv("PINECONE_FILTER");
//        Assume.assumeNotNull("No PINECONE_FILTER environment configured", host);
// todo ->   nResults: 10, ovvero limit, come parametro opzionale
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5) YIELD metadata, id",
                Map.of("host", HOST, /*"filter", filter, */"conf", emptyMap()),
                r -> {
                    System.out.println("r = " + r.next());
                    System.out.println("r = " + r.next());
                });
    }

    @Test
    public void getEmbeddingWithCreateIndex() {

        Map<String, Object> conf = Map.of(MAPPING_KEY, Map.of("embeddingProp", "vect", 
                "label", "Test", 
                "prop", "myId", 
                "id", "foo",
                "create", true));
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    System.out.println("r = " + r.next());
                    System.out.println("r = " + r.next());
                });

        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.stream(tx.schema().getIndexes())
                    .filter(i -> i.getIndexType().equals(IndexType.VECTOR))
                    .toList();
            assertEquals(1, indexes.size());
            assertEquals(List.of(Label.label("Test")), indexes.get(0).getLabels());
            assertEquals(List.of("vect"), indexes.get(0).getPropertyKeys());

            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints());
            assertEquals(1, constraints.size());
            assertEquals(Label.label("Test"), constraints.get(0).getLabel());
            assertEquals(List.of("myId"), constraints.get(0).getPropertyKeys());
        }

        testResult(db, "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                QdrantDbTest::vectorEntityAssertions);
    }

    @Test
    public void getEmbeddingWithCreateExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = Map.of(MAPPING_KEY, Map.of("embeddingProp", "vect",
                "label", "Test",
                "prop", "myId",
                "id", "foo"));
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    System.out.println("r = " + r.next());
                    System.out.println("r = " + r.next());
                });

        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.stream(tx.schema().getIndexes())
                    .filter(i -> i.getIndexType().equals(IndexType.VECTOR))
                    .toList();
            assertEquals(1, indexes.size());
            assertEquals(List.of(Label.label("Test")), indexes.get(0).getLabels());
            assertEquals(List.of("vect"), indexes.get(0).getPropertyKeys());

            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints());
            assertEquals(1, constraints.size());
            assertEquals(Label.label("Test"), constraints.get(0).getLabel());
            assertEquals(List.of("myId"), constraints.get(0).getPropertyKeys());
        }

        testResult(db, "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                QdrantDbTest::vectorEntityAssertions);
    }

    @Test
    public void getEmbeddingWithCreateRelIndex() {

        db.executeTransactionally("CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");
        
        Map<String, Object> conf = Map.of(MAPPING_KEY, Map.of("embeddingProp", "vect",
                "type", "TEST",
                "prop", "myId",
                "id", "foo",
                "create", true));
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    System.out.println("r = " + r.next());
                    System.out.println("r = " + r.next());
                });

        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.stream(tx.schema().getIndexes())
                    .filter(i -> i.getIndexType().equals(IndexType.VECTOR))
                    .toList();
            assertEquals(1, indexes.size());
            assertEquals(List.of(RelationshipType.withName("TEST")), indexes.get(0).getRelationshipTypes());
            assertEquals(List.of("vect"), indexes.get(0).getPropertyKeys());

            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints());
            assertEquals(1, constraints.size());
            assertEquals(RelationshipType.withName("TEST"), constraints.get(0).getRelationshipType());
            assertEquals(List.of("myId"), constraints.get(0).getPropertyKeys());
        }

        testResult(db, "MATCH (:Start)-[r:TEST]->(:End) RETURN properties(r) AS props ORDER BY r.myId", 
                QdrantDbTest::vectorEntityAssertions);
    }

    private static void vectorEntityAssertions(Result r) {
        ResourceIterator<Map> props = r.columnAs("props");
        Map next = props.next();
        assertEquals("Berlin", next.get("city"));
        assertEquals("one", next.get("myId"));
        assertTrue(next.get("vect") instanceof float[]);
        next = props.next();
        assertEquals("London", next.get("city"));
        assertEquals("two", next.get("myId"));
        assertTrue(next.get("vect") instanceof float[]);

        assertFalse(props.hasNext());
    }

}
