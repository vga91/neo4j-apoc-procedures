package apoc.vectordb;

import apoc.ml.RestAPIConfig;
import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.util.JsonUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.MapUtils;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ml.RestAPIConfig.JSON_PATH;
import static apoc.vectordb.VectorEmbeddingConfig.*;
import static apoc.util.ExtendedUtil.setProperties;
import static apoc.util.JsonUtil.OBJECT_MAPPER;
import static apoc.vectordb.VectorDbUtil.*;

/**
 * Base class
 */
public class VectorDb {

    @Context
    public URLAccessChecker urlAccessChecker;
    
    @Context
    public GraphDatabaseService db;
    
    @Context
    public Transaction tx;
    
    @Context
    public ProcedureCallContext procedureCallContext;
    
    @Procedure(value = "apoc.vectordb.custom.get", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.custom.get() - todo")
    public Stream<EmbeddingResult> get(@Name("hostOrKey") String hostOrKey,
                                       //   @Name("collection") String collection,
                                       //  @Name(value = "id", defaultValue = "") String id,
                                                @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        VectorEmbeddingConfig restAPIConfig = new VectorEmbeddingConfig(configuration, Map.of(), Map.of());
        return getEmbeddingResultStream(restAPIConfig, procedureCallContext, urlAccessChecker, db, tx);
    }
    
    public static Stream<EmbeddingResult> getEmbeddingResultStream(VectorEmbeddingConfig conf, ProcedureCallContext procedureCallContext, URLAccessChecker urlAccessChecker, GraphDatabaseService db, Transaction tx) throws Exception {
        List<String> fields = procedureCallContext.outputFields().toList();

        boolean hasEmbedding = fields.contains("embedding");
        boolean hasMetadata = fields.contains("metadata");
        Stream<Object> resultStream = executeRequest(conf, urlAccessChecker);

        VectorMappingConfig mapping = conf.getMapping();

        return resultStream
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(m -> {
                    // 
                    long id = (long) m.get(conf.getIdKey());
                    List<Double> embedding = hasEmbedding ? (List<Double>) m.get(conf.getEmbeddingKey()) : null;
                    Map<String, Object> metadata = hasMetadata ? (Map<String, Object>) m.get(conf.getMetadataKey()) : null;
                    // in case of get operation, e.g. http://localhost:52798/collections/{coll_name}/points with Qdrant db,
                    // score is not present
                    Double score = (Double) m.getOrDefault(conf.getScoreKey(), null);

                    handleMapping(tx, db, mapping, metadata, embedding);
                    return new EmbeddingResult(id, score, embedding, metadata);
                });
    }

    private static void handleMapping(Transaction tx, GraphDatabaseService db, VectorMappingConfig mapping, Map<String, Object> metadata, List<Double> embedding) {
        if (mapping.getProp() == null) {
            return;
        }
        if (MapUtils.isEmpty(metadata)) {
            throw new RuntimeException("To use mapping config, the metadata should not be empty. Make sure you execute `YIELD metadata` on the procedure");
        }
        if (mapping.getLabel() != null) {
            handleMappingNode(tx, db, mapping, metadata, embedding);
        } else if (mapping.getType() != null) {
            handleMappingRel(tx, db, mapping, metadata, embedding);
        } else {
            throw new RuntimeException("Mapping conf has to contain either label or type key");
        }
    }

    private static void handleMappingNode(Transaction tx, GraphDatabaseService db, VectorMappingConfig mapping, Map<String, Object> metadata, List<Double> embedding) {//, Object id, String prop, String label, String embeddingProp) {
        String query = "CREATE CONSTRAINT IF NOT EXISTS FOR (n:%s) REQUIRE n.%s IS UNIQUE"
                .formatted(mapping.getLabel(), mapping.getProp());
        db.executeTransactionally(query);

        try {
            Node node;
            try (Transaction transaction = db.beginTx()) {
                Object propValue = metadata.remove(mapping.getId());
                node = transaction.findNode(Label.label(mapping.getLabel()), mapping.getProp(), propValue);
                if (node == null && mapping.isCreate()) {
                    node = transaction.createNode(Label.label(mapping.getLabel()));
                }
                if (node != null) {
                    setProperties(node, metadata);
                }
                transaction.commit();
            }

            if (checkEmbeddingProp(mapping, embedding, node)) return;

            String vectorIndex = "CREATE VECTOR INDEX IF NOT EXISTS FOR (n:%s) ON (n.%s) OPTIONS {indexConfig: {`vector.dimensions`: %s, `vector.similarity_function`: '%s'}}"
                    .formatted(mapping.getLabel(), mapping.getEmbeddingProp(), embedding.size(), mapping.getSimilarity());
            db.executeTransactionally(vectorIndex);
            db.executeTransactionally("CALL db.create.setNodeVectorProperty($node, $key, $vector)",
                    Map.of("node", Util.rebind(tx, node), "key", mapping.getEmbeddingProp(), "vector", embedding));

        } catch (MultipleFoundException e) {
            throw new RuntimeException("Multiple nodes found");
        }
    }

    private static void handleMappingRel(Transaction tx, GraphDatabaseService db, VectorMappingConfig mapping, Map<String, Object> metadata, List<Double> embedding) {//, Object id, String prop, String type, String embeddingProp) {
        try {
            String query = "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:%s]-() REQUIRE (r.%s) IS UNIQUE"
                    .formatted(mapping.getType(), mapping.getProp());
            db.executeTransactionally(query);

            // in this case we cannot auto-create the rel, since we should have to define start and end node as well
            Relationship rel;
            try (Transaction transaction = db.beginTx()) {
                Object propValue = metadata.remove(mapping.getId());
                rel = transaction.findRelationship(RelationshipType.withName(mapping.getType()), mapping.getProp(), propValue);
                if (rel != null) {
                    setProperties(rel, metadata);
                }
                transaction.commit();
            }
            
            if (checkEmbeddingProp(mapping, embedding, rel)) return;
            
            String vectorIndex ="CREATE VECTOR INDEX IF NOT EXISTS FOR ()-[r:%s]-() ON (r.%s) OPTIONS {indexConfig: {`vector.dimensions`: %s, `vector.similarity_function`: '%s'}}"
                    .formatted(mapping.getType(), mapping.getEmbeddingProp(), embedding.size(), mapping.getSimilarity());
            db.executeTransactionally(vectorIndex);

            db.executeTransactionally("CALL db.create.setRelationshipVectorProperty($rel, $key, $vector)",
                    Map.of("rel", Util.rebind(tx, rel), "key", mapping.getEmbeddingProp(), "vector", embedding));

        } catch (MultipleFoundException e) {
            throw new RuntimeException("Multiple relationships found");
        }
    }

    private static boolean checkEmbeddingProp(VectorMappingConfig mapping, List<Double> embedding, Entity entity) {
        if (entity == null || mapping.getEmbeddingProp() == null) {
            return true;
        }

        if (embedding == null) {
            throw new RuntimeException("The embedding value is null. Make sure you execute `YIELD embedding` on the procedure");
        }
        return false;
    }


    // todo - write on pr: quite similar to apoc.load.jsonParams, but leverage the RestAPIConfig
    //  --> todo: maybe we can change it with a more generic naming, e.g. `apoc.restapi.custom(<conf>)`
    @Procedure("apoc.vectordb.custom")
    @Description("apoc.vectordb.custom() - todo")
    public Stream<ObjectResult> custom(@Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        // todo
        RestAPIConfig restAPIConfig = new RestAPIConfig(configuration);
        return executeRequest(restAPIConfig, urlAccessChecker)
//                .map(i -> (Map<String, Object>) i)
                .map(ObjectResult::new);
    }

    private static Stream<Object> executeRequest(RestAPIConfig apiConfig, URLAccessChecker urlAccessChecker) throws JsonProcessingException, MalformedURLException {
        Map<String, Object> headers = apiConfig.getHeaders();
        String body = OBJECT_MAPPER.writeValueAsString(apiConfig.getBody());
        return JsonUtil.loadJson(apiConfig.getEndpoint(), headers, body, apiConfig.getJsonPath(), true, List.of(), urlAccessChecker);
    }
    
    /*
    API QDRANT:
    - farle simili a pinecone
     */
    
    /*
    API CHROMA:
    - add
    - update
    - get
    - query
    - delete
     */
    
    /*
    API PINECONE:
    -query
    -fetch
    -upsert
    -delete
    -get index
    -custom
     */
    
    /* - TODO :procedure da fare      
        - query
        - filter
        -     
    */



    // todo - try it
//    @Context
//    public ProcedureCallContext procedureCallContext;
    

    
    // metadata --> contrassegno id e faccio sottobanco match node ... <-- configurabile però
    
    // altra cosa configurabile --> auto creazione di vector index
}
