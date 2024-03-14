package apoc.ml;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

// TODO - maybe move to `apoc.util` package?
public class RestAPIConfig {
    public static final String HEADERS_KEY = "headers";
    public static final String METHOD_KEY = "method";

    private final Map<String, Object> headers;

    public RestAPIConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }

        String httpMethod = (String) config.getOrDefault(METHOD_KEY, "POST");
        Map<String, Object> headerConf = (Map<String, Object>) config.getOrDefault(HEADERS_KEY, new HashMap<>());
        headerConf.putIfAbsent("content-type", "application/json");
        headerConf.putIfAbsent(METHOD_KEY, httpMethod);
        
        this.headers = headerConf;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }
}
