package apoc.ml.bedrock;

import java.util.List;
import java.util.Map;

public class BedrockResult {
    // todo
    // todo 2 : https://stackoverflow.com/questions/8571501/how-to-check-whether-a-string-is-base64-encoded-or-not
    record StabilityAiResult(String base64Image) {}

    // todo
    record AnthropicClaudeResult(String completion, String stopReason) {
        public AnthropicClaudeResult(Map<String, Object> map) {
            this((String) map.get("completion"), (String) map.get("stopReason"));
        }
    }

    // todo
    public record JurassicResult(Long id, List<Object> promptTokens, List<Object> completions) {
        public static JurassicResult from(Map<String, Object> map) {
            Long id = (Long) map.get("id");

            Map prompt = (Map) map.get("prompt");
            List<Object> promptTokens = (List<Object>) prompt.get("tokens");

            List<Object> completions = (List<Object>) map.get("completions");

            return new JurassicResult(id, promptTokens, completions);
        }
    }

    // todo
    record TitanEmbedding(Long inputTextTokenCount, List<Number> embedding) {}
}
