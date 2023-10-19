package apoc.ml.bedrock;

public class BedrockUtil {
    public static final String ALL = "*/*";
    public static final String JSON = "application/json";
    
    enum ModelId {
        JURASSIC_2_MID("ai21.j2-mid-v1"),
        JURASSIC_2_ULTRA("ai21.j2-ultra-v1"),

        TITAN_EMBEDDING_G1("amazon.titan-embed-text-v1"),
        TITAN_TEXT_G1_EXPRESS("amazon.titan-text-express-v1"),

        CLAUDE_V1("anthropic.claude-v1"),
        CLAUDE_V2("anthropic.claude-v2"),
        CLAUDE_INSTANT("anthropic.claude-instant-v1"),

        STABLE_DIFFUSION_XL("stability.stable-diffusion-xl-v0");

        private final String id;

        ModelId(String id) {
            this.id = id;
        }

        public String id() {
            return id;
        }
    }
}
