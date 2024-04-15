package apoc.ml.vectordb;

import apoc.ml.RestAPIConfig;
import apoc.result.MapResult;
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
import static apoc.ml.vectordb.VectorDb.VectorEmbeddingConfig.EMBEDDING_KEY;
import static apoc.ml.vectordb.VectorDb.VectorEmbeddingConfig.METADATA_KEY;
import static apoc.util.ExtendedUtil.setProperties;
import static apoc.util.JsonUtil.OBJECT_MAPPER;

/**
 * Base class
 */
public class VectorDb {
    
//    public interface Type {
//        public VectorEmbeddingConfig from(Map<String, Object> config, URLAccessChecker urlAccessChecker);
//    }
    
    public static class QdrantType /*implements Type*/ {

//        @Override
        public static VectorEmbeddingConfig from(Map<String, Object> config, ProcedureCallContext procedureCallContext,
                                                 List<Double> vector, Map<String, Object> filter, long limit) {
            List<String> fields = procedureCallContext.outputFields().toList();

//            Map<String, Object> body = conf.getBody();
//            config.putIfAbsent("metadata", 
            
            // "with_payload": true,
            // "with_vectors": true
            Map additionalBodies = Map.of("with_payload", fields.contains("metadata"),
                    "with_vectors", fields.contains("embedding"),
                    "vector", vector,
                    "filter", filter,
                    "limit", limit);
            
            config.putIfAbsent(EMBEDDING_KEY, "vector");
            config.putIfAbsent(METADATA_KEY, "payload");
            config.putIfAbsent(JSON_PATH, "result");
            
            // TODO - check it..
            VectorEmbeddingConfig conf = new VectorEmbeddingConfig(config, Map.of(), additionalBodies);

                    // todo - if fields.contains('metadata' -->  "with_payload": true,
            return conf;
        }
    }
    
    private static class VectorMappingConfig {
        private final Object id;
        private final String prop;

        private final String label;
        private final String type;
        private final String embeddingProp;
        private final String similarity;
        
        private final boolean create;
        
        public VectorMappingConfig(Map<String, Object> mapping) {
            if (mapping == null) {
                mapping = Collections.emptyMap();
            }
            this.id = mapping.get("id");
            this.prop = (String) mapping.get("prop");

            this.label = (String) mapping.get("label");
            this.type = (String) mapping.get("type");
            this.embeddingProp = (String) mapping.get("embeddingProp");
            
            this.similarity = (String) mapping.getOrDefault("similarity", "cosine");
            
            this.create = Util.toBoolean(mapping.get("create"));
        }

        public Object getId() {
            return id;
        }

        public String getProp() {
            return prop;
        }

        public String getLabel() {
            return label;
        }

        public String getType() {
            return type;
        }

        public String getEmbeddingProp() {
            return embeddingProp;
        }

        public boolean isCreate() {
            return create;
        }

        public String getSimilarity() {
            return similarity;
        }
    }
    
    public static class VectorEmbeddingConfig extends RestAPIConfig {
        public static final String EMBEDDING_KEY = "embeddingKey";
        public static final String METADATA_KEY = "metadataKey";
        public static final String SCORE_KEY = "scoreKey";
        public static final String ID_KEY = "idKey";
        public static final String MAPPING_KEY = "mapping";
        
        private final String idKey;
        private final String embeddingKey;
        private final String metadataKey;
        private final String scoreKey;
        
        private final VectorMappingConfig mapping;

        public VectorEmbeddingConfig(Map<String, Object> config, Map<String, Object> additionalHeaders, Map<String, Object> additionalBodies) {
            super(config, additionalHeaders, additionalBodies);
            this.embeddingKey = (String) config.getOrDefault(EMBEDDING_KEY, "embedding");
            this.metadataKey = (String) config.getOrDefault(METADATA_KEY, "metadata");
            this.scoreKey = (String) config.getOrDefault(SCORE_KEY, "score");
            this.idKey = (String) config.getOrDefault(ID_KEY, "id");
            this.mapping = new VectorMappingConfig((Map<String, Object>) config.getOrDefault(MAPPING_KEY, Map.of()));//.getOrDefault(MAPPING_KEY, Map.of());
        }

        public String getIdKey() {
            return idKey;
        }

        public String getEmbeddingKey() {
            return embeddingKey;
        }

        public String getMetadataKey() {
            return metadataKey;
        }

        public String getScoreKey() {
            return scoreKey;
        }

        public VectorMappingConfig getMapping() {
            return mapping;
        }
    }

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
                                                @Name("collection") String collection,
                                                @Name(value = "id", defaultValue = "") String id,
//                                                         @Name("apiKey") String apiKey,
                                                @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        VectorEmbeddingConfig restAPIConfig = new VectorEmbeddingConfig(configuration, Map.of(), Map.of());
        return getEmbeddingResultStream(restAPIConfig, procedureCallContext, urlAccessChecker, db, tx);
    }
    
    public static Stream<EmbeddingResult> getEmbeddingResultStream(VectorEmbeddingConfig conf, ProcedureCallContext procedureCallContext, URLAccessChecker urlAccessChecker, GraphDatabaseService db, Transaction tx) throws Exception {
        List<String> fields = procedureCallContext.outputFields().toList();
//        fields.contains(

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
                    double o = (double) m.get(conf.getScoreKey());

                    handleMapping(tx, db, mapping, metadata, embedding);
                    // todo - mapping handling..
                    return new VectorDb.EmbeddingResult(id, o, embedding, metadata);
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
            handleMappingNode(tx, db, mapping, metadata, embedding);//, id, prop, label, embeddingProp);
        } else if (mapping.getType() != null) {
            handleMappingRel(tx, db, mapping, metadata, embedding);//, id, prop, type, embeddingProp);
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
//            }

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

    public static class EmbeddingResult {
        public final long id;
        public final double score;
        public final List<Double> embedding;
        public final Map<String, Object> metadata;

        public EmbeddingResult(long id, double score, List<Double> embedding, Map<String, Object> metadata) {
            // todo - check it...
            this.id = id;
//            this.text = text;
            this.embedding = embedding;
            this.score = score;
            this.metadata = metadata;
        }
    }


    // todo - write on pr: quite similar to apoc.load.jsonParams, but leverage the RestAPIConfig
    //  --> todo: maybe we can change it with a more generic naming, e.g. `apoc.restapi.custom(<conf>)`
    @Procedure("apoc.vectordb.custom")
    @Description("apoc.vectordb.custom() - todo")
    public Stream<MapResult> custom(@Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        // todo
        RestAPIConfig restAPIConfig = new RestAPIConfig(configuration);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(i -> (Map<String, Object>) i)
                .map(MapResult::new);
    }

    private static Stream<Object> executeRequest(RestAPIConfig apiConfig, URLAccessChecker urlAccessChecker) throws JsonProcessingException, MalformedURLException {
        Map<String, Object> headers = apiConfig.getHeaders();
        String body = OBJECT_MAPPER.writeValueAsString(apiConfig.getBody());
        return JsonUtil.loadJson(apiConfig.getEndpoint(), headers, body, apiConfig.getJsonPath(), true, List.of(), urlAccessChecker);
    }
    
    
//    public static class VectorDbConfig {
//        public static final String HEADERS_KEY = "headers";
//        public static final String BODY_KEY = "body";
//        
//        private final Map<String, Object> headers;
//        private final Map<String, Object> body;
//        private final String endpoint;
//        private final String jsonPath;
//        
//        protected VectorDbConfig(Map<String, Object> config) {
//            if (config == null) {
//                config = Map.of();
//            }
//
//            this.headers = (Map<String, Object>) config.getOrDefault(HEADERS_KEY, new HashMap<>());
//            this.body = (Map<String, Object>) config.getOrDefault(BODY_KEY, new HashMap<>());
//            this.endpoint = getEndpoint(config, getDefaultEndpoint(config));
//        }
//
//        public Map<String, Object> getHeaders() {
//            return headers;
//        }
//
//        public Map<String, Object> getBody() {
//            return body;
//        }
//    }
    
    
        /*
        todo - FARE ANCHE UNA PROCEDURA CUSTOM, 
        e testare con chroma (e qdrant) 
     */
    
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
