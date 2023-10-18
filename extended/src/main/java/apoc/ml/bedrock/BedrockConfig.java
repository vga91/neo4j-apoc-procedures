package apoc.ml.bedrock;

import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_AWS_KEY_ID;
import static apoc.ExtendedApocConfig.APOC_AWS_SECRET_KEY;

// todo: as a record?
public class BedrockConfig {
    public static final String MODEL_ID = "modelId";
    public static final String SECRET_KEY = "secretKey";
    public static final String KEY_ID = "keyId";
    public static final String REGION = "region";
    public static final String ENDPOINT = "endpoint";

    private final String keyId;
    private final String secretKey;
    // todo - documentare che con modelId va su endpoint --> https://bedrock-runtime.us-east-1.amazonaws.com/model/<MODEL_ID>/invoke
    //  e che endpoint ha la priorità
    private final String endpoint;
    private final String region;
    
    // todo - maybe local, non viene richiamata all'esterno..
//    private final String modelId;
    
    private Map<String, Object> headers;

    public BedrockConfig(Map<String, Object> config) {
        this(config, null);
    }
    
    public BedrockConfig(Map<String, Object> config, String defaultEndpoint) {
        config = config == null ? Map.of() : config;

        // todo - document it
        this.keyId = apocConfig().getString(APOC_AWS_KEY_ID, (String) config.get(KEY_ID));
        this.secretKey = apocConfig().getString(APOC_AWS_SECRET_KEY, (String) config.get(SECRET_KEY));

        // todo - extract it?
        if (defaultEndpoint == null) {
            String modelId = (String) config.get(MODEL_ID);
            if (modelId != null) {
                defaultEndpoint = String.format("https://bedrock-runtime.us-east-1.amazonaws.com/model/%s/invoke", modelId);
            }
        }
        
        this.endpoint = getEndpoint(config, defaultEndpoint);
        
        
        // todo - passo modelId: se è valorizzato metto String urlString = String.format("https://bedrock-runtime.us-east-1.amazonaws.com/model/%s/invoke", 
        //                modelId1.getId());
        
        //      se modelId NON è valorizzato faccio getEndpoint()
        //      se 
        
        this.region = (String) config.getOrDefault(REGION, extractRegionFromEndpoint());
        
        this.headers = (Map<String, Object>) config.getOrDefault("headers", Map.of());
    }

    private String extractRegionFromEndpoint() {
        String beforeDomainName = endpoint.split("\\.amazonaws\\.com/")[0];

        return beforeDomainName.substring(beforeDomainName.lastIndexOf(".") + 1);
    }

    private String getEndpoint(Map<String, Object> config, String defaultEndpoint) {
        
        
        
        String endpointConfig = (String) config.get(ENDPOINT);
        if (endpointConfig != null) {
            return endpointConfig;
        }
        if (defaultEndpoint != null) {
            return defaultEndpoint;
        }
        throw new RuntimeException("TODO.. ENDPOINT ERROR");
    }

    public String getKeyId() {
        return keyId;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }
}
