package apoc.ml.vectordb;

import apoc.ml.RestAPIConfig;
import apoc.result.MapResult;
import apoc.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ml.RestAPIConfig.JSON_PATH;
import static apoc.ml.vectordb.VectorDb.VectorEmbeddingConfig.EMBEDDING_KEY;
import static apoc.ml.vectordb.VectorDb.VectorEmbeddingConfig.METADATA_KEY;
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
    
    public static class VectorEmbeddingConfig extends RestAPIConfig {
        public static final String EMBEDDING_KEY = "embeddingKey";
        public static final String METADATA_KEY = "metadataKey";
        public static final String SCORE_KEY = "scoreKey";
        public static final String ID_KEY = "idKey";
        
        private final String idKey;
        private final String embeddingKey;
        private final String metadataKey;
        private final String scoreKey;

        public VectorEmbeddingConfig(Map<String, Object> config, Map<String, Object> additionalHeaders, Map<String, Object> additionalBodies) {
            super(config, additionalHeaders, additionalBodies);
            this.embeddingKey = (String) config.getOrDefault(EMBEDDING_KEY, "embedding");
            this.metadataKey = (String) config.getOrDefault(METADATA_KEY, "metadata");
            this.scoreKey = (String) config.getOrDefault(SCORE_KEY, "score");
            this.idKey = (String) config.getOrDefault(ID_KEY, "id");
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
    }

    @Context
    public URLAccessChecker urlAccessChecker;
    
    @Context
    public ProcedureCallContext procedureCallContext;
    // todo - posso fare che quando 
    // todo - EmbeddingResult simile a promptmapresult
    
    @Procedure("apoc.vectordb.custom.get")
    @Description("apoc.vectordb.custom.get() - todo")
    public Stream<EmbeddingResult> get(@Name("hostOrKey") String hostOrKey,
                                                @Name("collection") String collection,
                                                @Name(value = "id", defaultValue = "") String id,
//                                                         @Name("apiKey") String apiKey,
                                                @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        VectorEmbeddingConfig restAPIConfig = new VectorEmbeddingConfig(configuration, Map.of(), Map.of());
        return getEmbeddingResultStream(restAPIConfig, procedureCallContext, urlAccessChecker);
    }
    
    public static Stream<EmbeddingResult> getEmbeddingResultStream(VectorEmbeddingConfig conf, ProcedureCallContext procedureCallContext, URLAccessChecker urlAccessChecker) throws Exception {
        List<String> fields = procedureCallContext.outputFields().toList();
//        fields.contains(

        boolean hasEmbedding = fields.contains("embedding");
        boolean hasMetadata = fields.contains("metadata");
        Stream<Object> resultStream = executeRequest(conf, urlAccessChecker);
        return resultStream
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(m -> {
                    // 
                    long id = (long) m.get(conf.getIdKey());
                    List<Double> embedding = hasEmbedding ? (List<Double>) m.get(conf.getEmbeddingKey()) : null;
                    Map<String, Object> metadata = hasMetadata ? (Map<String, Object>) m.get(conf.getMetadataKey()) : null;
                    double o = (double) m.get(conf.getScoreKey());
                    return new VectorDb.EmbeddingResult(id, o, embedding, metadata);
                });
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
