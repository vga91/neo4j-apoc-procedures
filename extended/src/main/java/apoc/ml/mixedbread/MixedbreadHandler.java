package apoc.ml.mixedbread;


import java.util.HashMap;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_ML_MIXEDBREAD_URL;

public interface MixedbreadHandler {
    // -- constants
    String ENDPOINT_CONF_KEY = "endpoint";
    String MODEL_ID_KEY = "model";
    String DEFAULT_MODEL_ID = "mxbai-embed-large-v1";
    String MIXEDBREAD_BASE_URL = "https://api.mixedbread.ai/v1/";

    enum Type {
        EMBEDDING(new EmbeddingHandler()),
        CUSTOM(new CustomHandler());

        private final MixedbreadHandler handler;

        Type(MixedbreadHandler handler) {
            this.handler = handler;
        }

        public MixedbreadHandler get() {
            return handler;
        }
    }

    // -- interface methods
    String getDefaultEndpoint();
    Map<String, Object> getPayload(Map<String, Object> configuration, Object input);

    default String getEndpoint(Map<String, Object> config) {
        var endpoint = config.remove(ENDPOINT_CONF_KEY);
        if (endpoint != null) {
            return (String) endpoint;
        }

        return apocConfig().getString( APOC_ML_MIXEDBREAD_URL, getDefaultEndpoint() );
    }

    // -- concrete implementations

    class EmbeddingHandler implements MixedbreadHandler {
        @Override
        public String getDefaultEndpoint() {
            return MIXEDBREAD_BASE_URL + "embeddings";
        }

        @Override
        public Map<String, Object> getPayload(Map<String, Object> configuration, Object input) {
            var config = new HashMap<>(configuration);
            config.putIfAbsent(MODEL_ID_KEY, DEFAULT_MODEL_ID);
            config.put("input", input);
            return config;
        }
    }

    class CustomHandler implements MixedbreadHandler {
        // todo - test with this
        public static final String ERROR_MSG_MISSING_ENDPOINT = "The endpoint must be defined via config `%s` or via apoc.conf `%s`"
                .formatted(ENDPOINT_CONF_KEY, APOC_ML_MIXEDBREAD_URL);

        // todo - test with this
        public static final String ERROR_MSG_MISSING_MODELID = "todo ERROR";

        @Override
        public String getDefaultEndpoint() {
            throw new RuntimeException(ERROR_MSG_MISSING_ENDPOINT);
        }

        @Override
        public Map<String, Object> getPayload(Map<String, Object> configuration, Object input) {
            var config = new HashMap<>(configuration);
            Object modelId = config.get(MODEL_ID_KEY);
            if (modelId == null) {
                throw new RuntimeException(ERROR_MSG_MISSING_MODELID);
            }

            return config;
        }
    }
}