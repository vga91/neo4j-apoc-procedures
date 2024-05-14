package apoc.ml;

import apoc.util.Util;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.Map;

import static apoc.ml.Prompt.API_KEY_CONF;

public class RagConfig {
    public static final String EMBEDDINGS_CONF = "embeddings";
    public static final String GET_LABEL_TYPES_CONF = "getLabelTypes";
    public static final String TOP_K_CONF = "topK";
    
    private final boolean getLabelTypes;
    private final EmbeddingQuery embedding;
    private final Integer topK;
    private final String apiKey;
    private final Map<String, Object> confMap;

    public RagConfig(Map<String, Object> confMap) {
        if (confMap == null) {
            confMap = Map.of();
        }

        this.confMap = confMap;
        this.getLabelTypes = Util.toBoolean(confMap.getOrDefault(GET_LABEL_TYPES_CONF, true));
        String embeddingString = (String) confMap.getOrDefault(EMBEDDINGS_CONF, EmbeddingQuery.Type.FALSE.name());
        this.embedding = EmbeddingQuery.Type.valueOf(embeddingString).get();
        this.topK = Util.toInteger(confMap.getOrDefault(TOP_K_CONF, 40));
        this.apiKey = (String) confMap.get(API_KEY_CONF);
    }

    public boolean isGetLabelTypes() {
        return getLabelTypes;
    }

    public EmbeddingQuery getEmbedding() {
        return embedding;
    }

    public Integer getTopK() {
        return topK;
    }

    public String getApiKey() {
        return apiKey;
    }

    public Map<String, Object> getConfMap() {
        return confMap;
    }
    
    public interface EmbeddingQuery {
        Result getQuery(String queryOrIndex, String question, Transaction tx, RagConfig config);
    
        String BASE_EMBEDDING_QUERY = """
                    CALL apoc.ml.openai.embedding([$question], $key , $conf)
                    YIELD index, text, embedding
                    WITH text, embedding
                    """;
    
        default Map<String, Object> getParams(String queryOrIndex, String question, RagConfig config) {
            return Map.of("vectorIndex", queryOrIndex,
                    TOP_K_CONF, config.getTopK(),
                    "question", question,
                    "key", config.getApiKey(),
                    "conf", config.getConfMap());
        }
    
        enum Type {
            NODE(new Node()),
            REL(new Rel()),
            FALSE(new False());
    
            private final EmbeddingQuery embedding;
    
            Type(EmbeddingQuery embedding) {
                this.embedding = embedding;
            }
    
            public EmbeddingQuery get() {
                return embedding;
            }
        }
    
        class False implements EmbeddingQuery {
            @Override
            public Result getQuery(String queryOrIndex, String question, Transaction tx, RagConfig config) {
                return tx.execute(queryOrIndex);
            }
        }
    
        class Node implements EmbeddingQuery {
            @Override
            public Result getQuery(String queryOrIndex, String question, Transaction tx, RagConfig config) {
                return tx.execute(BASE_EMBEDDING_QUERY + """
                            CALL db.index.vector.queryNodes($vectorIndex, $topK, embedding) YIELD node
                            RETURN node""",
                        getParams(queryOrIndex, question, config));
            }
        }
    
        class Rel implements EmbeddingQuery {
            @Override
            public Result getQuery(String queryOrIndex, String question, Transaction tx, RagConfig config) {
                return tx.execute(BASE_EMBEDDING_QUERY + """
                                    CALL db.index.vector.queryRelationships($vectorIndex, $topK, embedding) YIELD relationship
                                    RETURN relationship""",
                        getParams(queryOrIndex, question, config));
            }
        }
    }
}

