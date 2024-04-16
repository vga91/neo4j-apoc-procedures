package apoc.vectordb;

import java.util.List;
import java.util.Map;

public class VectorDbUtil {
    
    public static class EmbeddingResult {
        public final Object id;
        public final Double score;
        public final List<Double> embedding;
        public final Map<String, Object> metadata;

        public EmbeddingResult(Object id, Double score, List<Double> embedding, Map<String, Object> metadata) {
            this.id = id;
            this.embedding = embedding;
            this.score = score;
            this.metadata = metadata;
        }
    }
}
