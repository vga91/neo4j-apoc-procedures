package apoc.ml.vectordb;

import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class
 */
public class VectorDb {
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
    @Context
    public ProcedureCallContext procedureCallContext;
    
    public static class EmbeddingResult {
        public final long index;
        public final double score;
//        public final String text;
        public final List<Double> embedding;

        public EmbeddingResult(long index, /*String text, */List<Double> embedding, double score) {
            this.index = index;
//            this.text = text;
            this.embedding = embedding;
            this.score = score;
        }
    }
    
    // metadata --> contrassegno id e faccio sottobanco match node ... <-- configurabile però
    
    // altra cosa configurabile --> auto creazione di vector index
}
