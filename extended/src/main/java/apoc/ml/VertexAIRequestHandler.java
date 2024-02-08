package apoc.ml;


import apoc.ApocConfig;
import org.apache.commons.io.output.NullAppendable;

import java.io.IOException;
import java.util.Formatter;
import java.util.Locale;
import java.util.Map;

import static apoc.ml.OpenAI.ENDPOINT_CONF_KEY;
import static apoc.ml.VertexAI.APOC_ML_VERTEXAI_URL;
import static apoc.ml.VertexAI.DEFAULT_REGION;

public abstract class VertexAIRequestHandler {
    private static final String BASE_URL = "https://%1$s-aiplatform.googleapis.com/v1/projects/%2$s/locations/%3$s/publishers/google/models/%4$s:%5$s";
    private static final String STREAM_RESOURCE = "streamGenerateContent";
    private static final String PREDICT_RESOURCE = "predict";
    
//        private final String defaultUrl;
//        private final String defaultUrl;

//        VertexAIHandler(String defaultUrl) {
//            this.defaultUrl = defaultUrl;
//        }

//    public abstract String getDefaultUrl();
    public abstract String getDefaultResource();

//        {
//            return defaultUrl;
//        }
//        
//        public abstract String getUrlTemplate();

    private String getUrlTemplate(Map<String, Object> procConfig, ApocConfig apocConfig) {
        String urlTemplate = (String) procConfig.getOrDefault(ENDPOINT_CONF_KEY,
                apocConfig.getString(APOC_ML_VERTEXAI_URL, System.getProperty(APOC_ML_VERTEXAI_URL, BASE_URL)));


//        if (urlTemplate == null) {
//            throw new RuntimeException("errore todo ");
//        }
        return urlTemplate;
    }

    
    public abstract Map<String, Object> getBody(Object inputs, Map<String, Object> parameters);
//    {
        // todo cambiare partendo da questo--> Map<String, Object> data = Map.of("instances", inputs
//    }


    public String getFullUrl(Map<String, Object> configuration, ApocConfig apocConfig, String defaultModel, String project) {
        String model = configuration.getOrDefault("model", defaultModel).toString();
        String region = configuration.getOrDefault("region", DEFAULT_REGION).toString();
        String resource = configuration.getOrDefault("resource", getDefaultResource()).toString();
        
        String endpoint = getUrlTemplate(configuration, apocConfig);
//        if (endpoint == null && resource == null) {
//            throw new RuntimeException("TODO: fkfkfkfkfk");
//        }

//        Formatter aaa = new Formatter(new Appendable() {
//            @Override
//            public Appendable append(CharSequence csq) throws IOException {
//                return null;
//            }
//
//            @Override
//            public Appendable append(CharSequence csq, int start, int end) throws IOException {
//                return null;
//            }
//
//            @Override
//            public Appendable append(char c) throws IOException {
//                return null;
//            }
//        }, Locale.getDefault()).format("%1$s %1$s %2$s b", "aaa", null);

        // String.format("%1$s %1$s %2$s b", "xxx", "yyy", "zzz")

        return String.format(endpoint,
                region, project, region, model, resource);
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
    
    