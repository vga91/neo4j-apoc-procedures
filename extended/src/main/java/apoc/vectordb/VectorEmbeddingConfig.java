package apoc.vectordb;

import apoc.ml.RestAPIConfig;

import java.util.Map;

public class VectorEmbeddingConfig extends RestAPIConfig {
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
