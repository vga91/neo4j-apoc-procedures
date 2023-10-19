package apoc.ml.bedrock;

import java.util.List;
import java.util.Map;

public class BedrockInvokeResult {
    public record StabilityAi(String base64Image) {
        public static StabilityAi from(Object object) {
            Map<String, Object> map = (Map<String, Object>) object;
            
            String base64 = (String) map.get("base64");
            
            return new StabilityAi(base64);
        }
    }

    public record AnthropicClaude(String completion, String stopReason) {
        public static AnthropicClaude from(Object object) {
            Map<String, Object> map = (Map<String, Object>) object;
            
            String completion = (String) map.get("completion");
            String stopReason = (String) map.get("stopReason");
            
            return new AnthropicClaude(completion, stopReason);
        }
    }

    public record TitanEmbedding(Long inputTextTokenCount, List<Number> embedding) {
        public static TitanEmbedding from(Object object) {
            Map<String, Object> map = (Map<String, Object>) object;
            
            Long inputTextTokenCount = (Long) map.get("inputTextTokenCount");
            List<Number> embedding = (List<Number>) map.get("embedding");
            
            return new TitanEmbedding(inputTextTokenCount, embedding);
        }
    }
    
    public record Jurassic(Long id, List<Object> promptTokens, List<Object> completions) {
        public static Jurassic from(Object object) {
            Map<String, Object> map = (Map<String, Object>) object;
            
            Long id = (Long) map.get("id");

            Map prompt = (Map) map.get("prompt");
            List<Object> promptTokens = (List<Object>) prompt.get("tokens");

            List<Object> completions = (List<Object>) map.get("completions");

            return new Jurassic(id, promptTokens, completions);
        }
    }
}
