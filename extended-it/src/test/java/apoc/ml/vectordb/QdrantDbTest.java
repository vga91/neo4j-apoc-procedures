package apoc.ml.vectordb;

import apoc.es.ElasticSearch;
import apoc.util.TestUtil;
import io.qdrant.client.grpc.Points;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.qdrant.QdrantContainer;

import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.VectorParams;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testCall;
import static io.qdrant.client.PointIdFactory.id;
import static io.qdrant.client.ValueFactory.value;
import static io.qdrant.client.VectorsFactory.vectors;
import static java.util.Collections.emptyMap;

import io.qdrant.client.grpc.Points.PointStruct;
import io.qdrant.client.grpc.Points.UpdateResult;

import java.util.List;
import java.util.Map;

// todo - inizialmente popolo il db tramite questo https://qdrant.tech/documentation/quick-start/#add-vectors
//  con testImplementation
//  poi vedo se fare nuove procedure, oppure una custom..


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

    @BeforeClass
    public static void setUp() throws Exception {
        qdrant.start();
        TestUtil.registerProcedure(db, VectorDb.class, Qdrant.class);

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);

        // -- todo - do these via procedures
        
// The Java client uses Qdrant's GRPC interface
//        QdrantClient client = new QdrantClient(
//                QdrantGrpcClient.newBuilder("localhost", qdrant.getMappedPort(6333), false).build());
//        client.createCollectionAsync("test_collection",
//                VectorParams.newBuilder().setDistance(Distance.Dot).setSize(4).build()).get();

        String endpoint = db.executeTransactionally("""
                        CALL apoc.vectordb.custom({
                        endpoint: $endpoint,
                        body: {
                            vectors: {
                              size: 4,
                              distance: "Cosine"
                            }
                        }, method: 'PUT'})""", Map.of("endpoint", "http://localhost:" + qdrant.getMappedPort(6333) + "/collections/test_collection"),
                Result::resultAsString);

        String endpoint1 = db.executeTransactionally("""
                        CALL apoc.vectordb.custom({
                        endpoint: $endpoint,
                        body: {
                              points: [
                                {
                                  id: 1,
                                  vector: [0.05, 0.61, 0.76, 0.74],
                                  payload: {city: "Berlin"}
                                },
                                {
                                  id: 2,
                                  vector: [0.19, 0.81, 0.75, 0.11],
                                  payload: {city: "London"}
                                }
                            ]
                        }, method: 'PUT'})""", Map.of("endpoint", "http://localhost:" + qdrant.getMappedPort(6333) + "/collections/test_collection/points"),
                Result::resultAsString);

        System.out.println("endpoint1 = " + endpoint1);

//        UpdateResult operationInfo =
//                client.upsertAsync(
//                            "test_collection",
//                            List.of(
//                                    PointStruct.newBuilder()
//                                            .setId(id(1))
//                                            .setVectors(vectors(0.05f, 0.61f, 0.76f, 0.74f))
//                                            .putAllPayload(Map.of("city", value("Berlin")))
//                                            .build(),
//                                    PointStruct.newBuilder()
//                                            .setId(id(2))
//                                            .setVectors(vectors(0.19f, 0.81f, 0.75f, 0.11f))
//                                            .putAllPayload(Map.of("city", value("London")))
//                                            .build(),
//                                    PointStruct.newBuilder()
//                                            .setId(id(3))
//                                            .setVectors(vectors(0.36f, 0.55f, 0.47f, 0.94f))
//                                            .putAllPayload(Map.of("city", value("Moscow")))
//                                            .build()))
//                    // Truncated
//                    .get();
        // -- todo - do these via procedures
        
    }


    @Test
    public void getEmbedding() {
//        String filter = System.getenv("PINECONE_FILTER");
//        Assume.assumeNotNull("No PINECONE_FILTER environment configured", host);
// todo ->   nResults: 10, ovvero limit, come parametro opzionale
        testCall(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5)",
                Map.of("host", "localhost:" + qdrant.getMappedPort(6333), /*"filter", filter, */"conf", emptyMap()),
                r -> {
                    System.out.println("r = " + r);
                });
    }
    
    @Test
    public void getEmbeddingWithYield() {
//        String filter = System.getenv("PINECONE_FILTER");
//        Assume.assumeNotNull("No PINECONE_FILTER environment configured", host);
// todo ->   nResults: 10, ovvero limit, come parametro opzionale
        testCall(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5) YIELD metadata, id",
                Map.of("host", "localhost:" + qdrant.getMappedPort(6333), /*"filter", filter, */"conf", emptyMap()),
                r -> {
                    System.out.println("r = " + r);
                });
    }
    
}
