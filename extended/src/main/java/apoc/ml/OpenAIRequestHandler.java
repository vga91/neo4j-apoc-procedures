package apoc.ml;


import apoc.ApocConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ExtendedApocConfig.APOC_ML_OPENAI_AZURE_VERSION;
import static apoc.ExtendedApocConfig.APOC_ML_OPENAI_URL;
import static apoc.ml.OpenAI.API_VERSION_CONF_KEY;
import static apoc.ml.OpenAI.ENDPOINT_CONF_KEY;

abstract class OpenAIRequestHandler {

    private final String defaultUrl;

    public OpenAIRequestHandler(String defaultUrl) {
        this.defaultUrl = defaultUrl;
    }

    public String getDefaultUrl() {
        return defaultUrl;
    }
    public abstract String getApiVersion(Map<String, Object> configuration, ApocConfig apocConfig);
    public abstract void addApiKey(Map<String, Object> headers, String apiKey);

    public String getEndpoint(Map<String, Object> procConfig, ApocConfig apocConfig) {
        return (String) procConfig.getOrDefault(ENDPOINT_CONF_KEY,
                apocConfig.getString(APOC_ML_OPENAI_URL, System.getProperty(APOC_ML_OPENAI_URL, getDefaultUrl())));
    }

    public String getFullUrl(String method, Map<String, Object> procConfig, ApocConfig apocConfig) {
        return Stream.of(getEndpoint(procConfig, apocConfig), adaptMethod(method), getApiVersion(procConfig, apocConfig))
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.joining("/"));
    }
    
    public void addBodyEntries(String key, Object inputs, String model, Map<String, Object> config) {
        config.putIfAbsent("model", model);
        config.put(key, inputs);
    }
    
    public String adaptJsonPath(String jsonPath) {
        return jsonPath;
    }
    
    public String adaptMethod(String method) {
        return method;
    }

    enum Type {
        AZURE(new Azure(null)),
        LOCALAI(new OpenAi(null)),
        COHERE(new Cohere()),
        HUGGINGFACE(new HuggingFace()),
        OPENAI(new OpenAi("https://api.openai.com/v1"));

        private final OpenAIRequestHandler handler;
        Type(OpenAIRequestHandler handler) {
            this.handler = handler;
        }

        public OpenAIRequestHandler get() {
            return handler;
        }
    }

    private static class Cohere extends OpenLM {
        public Cohere() {
            super(null);
            // todo
//            super("defaultUrl TODO");
        }

//        @Override
//        public void addBodyEntries(String key, Object inputs, String model, Map<String, Object> config) {
////            config.put("inputs", "non so"); TODO
//        }
    }
    
    private static class HuggingFace extends OpenLM {
        public HuggingFace() {
            super(null);
//            super("https://api-inference.huggingface.co/models");
        }

        @Override
        public void addBodyEntries(String key, Object inputs, String model, Map<String, Object> config) {
//            Object inputsValue = config.remove("inputs");
            config.putIfAbsent("inputs", inputs);

//            config.put("inputs", "non so");
        }
    }

    private static class OpenLM extends OpenAi {
        public OpenLM(String defaultUrl) {
            super(defaultUrl);
        }
        

        @Override
        public String adaptJsonPath(String jsonPath) {
            return "$[0]";
        }

        @Override
        public String adaptMethod(String method) {
            return "";
        }
    }
    
//    static class LocalAi extends OpenAi {
//
//        public LocalAi() {
//            super(n);
//        }
//    }

    static class Azure extends OpenAIRequestHandler {

        public Azure(String defaultUrl) {
            super(defaultUrl);
        }

        @Override
        public String getApiVersion(Map<String, Object> configuration, ApocConfig apocConfig) {
            return "?api-version=" + configuration.getOrDefault(API_VERSION_CONF_KEY, apocConfig.getString(APOC_ML_OPENAI_AZURE_VERSION));
        }

        @Override
        public void addApiKey(Map<String, Object> headers, String apiKey) {
            headers.put("api-key", apiKey);
        }
    }

    static class OpenAi extends OpenAIRequestHandler {

        public OpenAi(String defaultUrl) {
            super(defaultUrl);
        }

        @Override
        public String getApiVersion(Map<String, Object> configuration, ApocConfig apocConfig) {
            return "";
        }

        @Override
        public void addApiKey(Map<String, Object> headers, String apiKey) {
            headers.put("Authorization", "Bearer " + apiKey);
        }
    }
}
