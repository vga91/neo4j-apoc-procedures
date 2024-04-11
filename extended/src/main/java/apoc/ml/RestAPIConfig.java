package apoc.ml;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

// TODO - maybe move to `apoc.util` package?
public class RestAPIConfig {
    public static final String HEADERS_KEY = "headers";
    public static final String METHOD_KEY = "method";
    public static final String ENDPOINT_KEY = "endpoint";
    public static final String JSON_PATH = "jsonPath";
    public static final String BODY_KEY = "body";
    
    private final Map<String, Object> headers;
    private final Map<String, Object> body;
    private final String endpoint;
    private final String jsonPath;

    public RestAPIConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }

        String httpMethod = (String) config.getOrDefault(METHOD_KEY, "POST");
        Map<String, Object> headerConf = (Map<String, Object>) config.getOrDefault(HEADERS_KEY, new HashMap<>());
        headerConf.putIfAbsent("content-type", "application/json");
        headerConf.putIfAbsent(METHOD_KEY, httpMethod);
        
        this.headers = headerConf;

        this.endpoint = (String) config.getOrDefault(ENDPOINT_KEY, getDefaultEndpoint());

        this.jsonPath = (String) config.get(JSON_PATH);
        this.body = (Map<String, Object>) config.getOrDefault(BODY_KEY, new HashMap<>());
    }

    public String getDefaultEndpoint() {
        throw new RuntimeException("todo - error, endpoint must be specified");
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public Map<String, Object> getBody() {
        return body;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getJsonPath() {
        return jsonPath;
    }
}
