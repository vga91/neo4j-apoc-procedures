package apoc.load;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ConnectionConfig {
    public static final String COMPRESSION = "compression";

    private final String keyStoreUrl;
    private final String compressionAlgo;

    public ConnectionConfig(Map<String, Object> config, String defaultCompression) {
        Objects.requireNonNull("Field `name` should be defined")
        this.compressionAlgo = (String) config.getOrDefault(COMPRESSION, defaultCompression);
        this.charset = Charset.forName((String) config.getOrDefault(CHARSET, UTF_8.name()));
    }

}
