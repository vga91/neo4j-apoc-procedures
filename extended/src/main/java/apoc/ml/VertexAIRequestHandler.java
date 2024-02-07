package apoc.ml;


import apoc.ApocConfig;

import java.util.Map;

import static apoc.ml.OpenAI.ENDPOINT_CONF_KEY;
import static apoc.ml.VertexAI.APOC_ML_VERTEXAI_URL;
import static apoc.ml.VertexAI.DEFAULT_REGION;

public abstract class VertexAIRequestHandler {
//        private final String defaultUrl;
//        private final String defaultUrl;

//        VertexAIHandler(String defaultUrl) {
//            this.defaultUrl = defaultUrl;
//        }

    public abstract String getDefaultUrl();
    public abstract String getDefaultResource();

//        {
//            return defaultUrl;
//        }
//        
//        public abstract String getUrlTemplate();

    private String getUrlTemplate(Map<String, Object> procConfig, ApocConfig apocConfig) {
        String urlTemplate = (String) procConfig.getOrDefault(ENDPOINT_CONF_KEY,
                apocConfig.getString(APOC_ML_VERTEXAI_URL, System.getProperty(APOC_ML_VERTEXAI_URL, getDefaultUrl())));


//        if (urlTemplate == null) {
//            throw new RuntimeException("errore todo ");
//        }
        return urlTemplate;
    }

    
    public String getBody(Object in) {
        // todo cambiare partendo da questo--> Map<String, Object> data = Map.of("instances", inputs
    }


    public String getFullUrl(Map<String, Object> configuration, ApocConfig apocConfig, String defaultModel, String project) {
        String endpoint = getUrlTemplate(configuration, apocConfig);

        String model = configuration.getOrDefault("model", defaultModel).toString();
        String region = configuration.getOrDefault("region", DEFAULT_REGION).toString();
        String resource = configuration.getOrDefault("resource", getDefaultResource()).toString();
        
        if (endpoint == null && resource == null) {
            throw new RuntimeException("TODO: fkfkfkfkfk");
        }

        return String.format(endpoint, region, project, region, model);
//            return Stream.of(getEndpoint(procConfig, apocConfig), method, getApiVersion(procConfig, apocConfig))
//                    .filter(StringUtils::isNotBlank)
//                    .collect(Collectors.joining("/"));
    }

    enum Type {
        PREDICT(new Predict()),
        STREAM(new Stream()),
        CUSTOM(new Custom());
//            PREDICT(BASE_URL),
//            STREAM(BASE_URL_STREAM),
//            CUSTOM(null);

//            private final String defaultUrl;
//            
//            Type(String defaultUrl) {
//                this.defaultUrl = defaultUrl;
//            }

        private final VertexAIRequestHandler handler;
        Type(VertexAIRequestHandler handler) {
            this.handler = handler;
        }

        public VertexAIRequestHandler get() {
            return handler;
        }

//            public String getUrlTemplate(Map<String, Object> procConfig, ApocConfig apocConfig) {
//                String urlTemplate = (String) procConfig.getOrDefault(ENDPOINT_CONF_KEY,
//                        apocConfig.getString(APOC_ML_VERTEXAI_URL, System.getProperty(APOC_ML_VERTEXAI_URL, defaultUrl)));
//                if (urlTemplate == null) {
//                    throw new RuntimeException("errore todo ");
//                }
//                return urlTemplate;
//            }
//
//            public String getFullUrl(Map<String, Object> configuration, ApocConfig apocConfig, String defaultModel, String project) {
//                String endpoint = getUrlTemplate(configuration, apocConfig);
//
//                String model = configuration.getOrDefault("model", defaultModel).toString();
//                String region = configuration.getOrDefault("region", DEFAULT_REGION).toString();
//
//                return String.format(endpoint, region, project, region, model);
////            return Stream.of(getEndpoint(procConfig, apocConfig), method, getApiVersion(procConfig, apocConfig))
////                    .filter(StringUtils::isNotBlank)
////                    .collect(Collectors.joining("/"));
//            }
    }

    private static class Predict extends VertexAIRequestHandler {
        @Override
        public String getDefaultUrl() {
            return "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:%s";
        }

        @Override
        public String getDefaultResource() {
            return "predict";
        }
    }

    private static class Stream extends VertexAIRequestHandler {
        @Override
        public String getDefaultUrl() {
            return null;
        }

        @Override
        public String getDefaultResource() {
            return null;
        }


        @Override
        public String getFullUrl(Map<String, Object> configuration, ApocConfig apocConfig, String defaultModel, String project) {
            String fullUrl = super.getFullUrl(configuration, apocConfig, defaultModel, project);
            
            // TODO...
            return fullUrl;
        }
    }

    private static class Custom extends VertexAIRequestHandler {
        @Override
        public String getDefaultUrl() {
            return null;
        }

        @Override
        public String getDefaultResource() {
            return null;
        }
    }
}
    
    