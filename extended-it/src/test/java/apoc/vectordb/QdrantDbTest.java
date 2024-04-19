package apoc.vectordb;

import apoc.util.TestUtil;
import apoc.util.collection.Iterables;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.qdrant.QdrantContainer;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testResult;
import static apoc.vectordb.VectorDbTestUtil.assertBerlinVector;
import static apoc.vectordb.VectorDbTestUtil.assertLondonVector;
import static apoc.vectordb.VectorDbTestUtil.vectorEntityAssertions;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;

public class QdrantDbTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static final QdrantContainer qdrant = new QdrantContainer("qdrant/qdrant:v1.7.4");
    public static String HOST;

    @BeforeClass
    public static void setUp() throws Exception {
        qdrant.start();

        HOST = "localhost:" + qdrant.getMappedPort(6333);
        TestUtil.registerProcedure(db, VectorDb.class, Qdrant.class);

//        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
//        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);


        testCall(db, "CALL apoc.vectordb.qdrant.createCollection($host, 'test_collection', 'Cosine', 4)",
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });

        testCall(db, """
                        CALL apoc.vectordb.qdrant.upsert($host, 'test_collection',
                        [
                            {id: 1, embedding: [0.05, 0.61, 0.76, 0.74], metadata: {city: "Berlin", foo: "one"}},
                            {id: 2, embedding: [0.19, 0.81, 0.75, 0.11], metadata: {city: "London", foo: "two"}}
                        ])
                        """,
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });

    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCall(db, "CALL apoc.vectordb.qdrant.deleteCollection($host, 'test_collection')",
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals(true, value.get("result"));
                });
    }
    
    @Before
    public void after() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraints().forEach(ConstraintDefinition::drop);
            tx.schema().getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        }
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void getEmbeddings() {
        testResult(db, "CALL apoc.vectordb.qdrant.get($host, 'test_collection', [1]) ",
                Map.of("host", HOST),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("embedding"));
                });
    }

    @Test
    public void deleteVector() {
        testCall(db, """
                        CALL apoc.vectordb.qdrant.upsert($host, 'test_collection',
                        [
                            {id: 3, embedding: [0.19, 0.81, 0.75, 0.11], metadata: {foo: "baz"}}
                        ])
                        """,
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });
        
        testCall(db, "CALL apoc.vectordb.qdrant.delete($host, 'test_collection', [3]) ",
                Map.of("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });
    }

    @Test
    public void getEmbedding() {
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5)",
                Map.of("host", HOST, "conf", emptyMap()),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("embedding"));

                    row = r.next();
                    assertLondonVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("embedding"));
                });
    }


    @Test
    public void getEmbeddingWithYield() {
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5) YIELD metadata, id",
                Map.of("host", HOST, /*"filter", filter, */"conf", emptyMap()),
                r -> {
                    assertBerlinVector(r.next());
                    assertLondonVector(r.next());
                });
    }


    @Test
    public void getEmbeddingWithFilter() {
        testResult(db, """
                        CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], 
                        { must: 
                            [ { key: "city", match: { value: "London" } } ] 
                        }, 
                        5) YIELD metadata, id""",
                Map.of("host", HOST, "conf", emptyMap()),
                r -> {
                    assertLondonVector(r.next());
                });
    }

    @Test
    public void getEmbeddingWithLimit() {
        testResult(db, """
                        CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 1) YIELD metadata, id""",
                Map.of("host", HOST, /*"filter", filter, */"conf", emptyMap()),
                r -> {
                    assertBerlinVector(r.next());
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
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("embedding"));

                    row = r.next();
                    assertLondonVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("embedding"));
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
                r -> vectorEntityAssertions(r, true));

        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("embedding"));

                    row = r.next();
                    assertLondonVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("embedding"));
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

        testCallCount(db, "MATCH (n:Test) RETURN n", 4);
    }

    @Test
    public void getEmbeddingWithCreateIndexUsingExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = Map.of(MAPPING_KEY, Map.of("embeddingProp", "vect",
                "label", "Test",
                "prop", "myId",
                "id", "foo"));
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("embedding"));

                    row = r.next();
                    assertLondonVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("embedding"));
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
                r -> vectorEntityAssertions(r, false));
    }

    @Test
    public void getEmbeddingWithCreateRelIndex() {

        db.executeTransactionally("CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");
        
        Map<String, Object> conf = Map.of(MAPPING_KEY, Map.of("embeddingProp", "vect",
                "type", "TEST",
                "prop", "myId",
                "id", "foo"));
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("embedding"));

                    row = r.next();
                    assertLondonVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("embedding"));
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
                r -> vectorEntityAssertions(r, false));
    }

}
