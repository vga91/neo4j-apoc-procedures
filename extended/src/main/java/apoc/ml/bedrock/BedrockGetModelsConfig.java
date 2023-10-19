package apoc.ml.bedrock;

import java.util.Map;

public class BedrockGetModelsConfig extends BedrockConfig {
    enum TypeGet {
        CUSTOM("custom-models"),
        FOUNDATION("foundation-models");

        private final String path;

        TypeGet(String path) {
            this.path = path;
        }

        public static String from(String value) {
            for (TypeGet typeGet: TypeGet.values()) {
                if (typeGet.name().equals(value)) {
                    return typeGet.path;
                }
            }
            return TypeGet.FOUNDATION.path;
        }
    }

    public static final String TYPE_GET = "typeGet";

    public BedrockGetModelsConfig(Map<String, Object> config) {
        super(config);
    }

    @Override
    String getDefaultEndpoint(Map<String, Object> config) {
        String typeGet = TypeGet.from((String) config.get(TYPE_GET));
        return "https://bedrock.us-east-1.amazonaws.com/" + typeGet;
    }

    @Override
    String getDefaultMethod() {
        return "GET";
    }
}
