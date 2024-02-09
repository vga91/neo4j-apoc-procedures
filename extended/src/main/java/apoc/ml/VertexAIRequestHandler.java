package apoc.ml;


import apoc.ApocConfig;

import java.util.Map;

import static apoc.ExtendedApocConfig.APOC_ML_VERTEXAI_URL;
import static apoc.ml.VertexAI.DEFAULT_REGION;
import static org.apache.commons.lang3.StringUtils.isBlank;

public abstract class VertexAIRequestHandler {
    private static final String BASE_URL = "https://%1$s-aiplatform.googleapis.com/v1/projects/%2$s/locations/%1$s/publishers/google/models/%3$s:%4$s";
    public static final String ENDPOINT_CONF_KEY = "endpoint";
    private static final String STREAM_RESOURCE = "streamGenerateContent";
    private static final String PREDICT_RESOURCE = "predict";
    
    public abstract String getDefaultResource();
    
    public abstract Map<String, Object> getBody(Object inputs, Map<String, Object> parameters);
    
    public String getFullUrl(Map<String, Object> configuration, ApocConfig apocConfig, String defaultModel, String project) {
        String model = configuration.getOrDefault("model", defaultModel).toString();
        String region = configuration.getOrDefault("region", DEFAULT_REGION).toString();
        String resource = configuration.getOrDefault("resource", getDefaultResource()).toString();
        
        String endpoint = getUrlTemplate(configuration, apocConfig);
        
        if (isBlank(endpoint) && isBlank(project)) {
                throw new IllegalArgumentException("Either project parameter or endpoint config. must not be empty");
        }
        return String.format(endpoint, region, project, model, resource);
    }

    private String getUrlTemplate(Map<String, Object> procConfig, ApocConfig apocConfig) {
        return (String) procConfig.getOrDefault(ENDPOINT_CONF_KEY,
                apocConfig.getString(APOC_ML_VERTEXAI_URL, System.getProperty(APOC_ML_VERTEXAI_URL, BASE_URL)));
    }

    enum Type {
        PREDICT(new Predict()),
        STREAM(new Stream()),
        CUSTOM(new Custom());
        
        private final VertexAIRequestHandler handler;
        Type(VertexAIRequestHandler handler) {
            this.handler = handler;
        }

        public VertexAIRequestHandler get() {
            return handler;
        }
    }

    private static class Predict extends VertexAIRequestHandler {

        @Override
        public String getDefaultResource() {
            return PREDICT_RESOURCE;
        }

        @Override
        public Map<String, Object> getBody(Object inputs, Map<String, Object> parameters) {
            return Map.of("instances", inputs, "parameters", parameters);
        }
    }

    private static class Stream extends VertexAIRequestHandler {

        @Override
        public String getDefaultResource() {
            return STREAM_RESOURCE;
        }

        @Override
        public Map<String, Object> getBody(Object inputs, Map<String, Object> parameters) {
            return Map.of("contents", inputs, "generation_config", parameters);
        }
    }

    private static class Custom extends VertexAIRequestHandler {

        @Override
        public String getDefaultResource() {
            return STREAM_RESOURCE;
        }

        @Override
        public Map<String, Object> getBody(Object inputs, Map<String, Object> parameters) {
            return (Map<String, Object>) inputs;
        }
    }
}
    
    