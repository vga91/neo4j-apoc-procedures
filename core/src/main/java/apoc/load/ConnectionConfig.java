package apoc.load;

import javax.net.ssl.TrustManagerFactory;
import java.util.Collections;
import java.util.Map;

abstract public class ConnectionConfig {
    public static final String KEYSTORE_URL_KEY = "keyStoreUrl";
    public static final String KEYSTORE_PWD_KEY = "keyStorePassword";
    public static final String KEYSTORE_TYPE_KEY = "keyStoreType";
    public static final String SECURE_PROTOCOL_KEY = "secureProtocol";
    public static final String TRUST_ALGO_KEY = "secureProtocol";

    private final String keyStoreUrl;
    private final String keyStorePassword;
    private final String keyStoreType;
    private final String secureProtocol;
    private final String trustManagerAlgorithm;

    public ConnectionConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.keyStoreUrl = (String) config.get(KEYSTORE_URL_KEY);
        this.keyStorePassword = (String) config.get(KEYSTORE_PWD_KEY);
        if (keyStoreUrl == null ^ keyStorePassword == null) {
            throw new RuntimeException("You have to define both keyStoreUrl and keyStorePassword, or none of them");
        }
        this.keyStoreType = (String) config.getOrDefault(KEYSTORE_TYPE_KEY, "jks");
        this.secureProtocol = (String) config.getOrDefault(SECURE_PROTOCOL_KEY, "TLS");
        this.trustManagerAlgorithm = (String) config.getOrDefault(TRUST_ALGO_KEY, TrustManagerFactory.getDefaultAlgorithm());
    }

    public String getKeyStoreUrl() {
        return keyStoreUrl;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public String getSecureProtocol() {
        return secureProtocol;
    }

    public String getTrustManagerAlgorithm() {
        return trustManagerAlgorithm;
    }
}
