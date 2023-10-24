package apoc.ml.bedrock;

import java.util.List;
import java.util.Map;

public class BedrockInvokeResult {
    public record Image(String base64Image) {
        public static Image from(Map<String, Object> map) {
            String base64 = (String) map.get("base64");
            
            return new Image(base64);
        }
    }

    public record TitanEmbedding(Long inputTextTokenCount, String text, List<Double> embedding) {
        public static TitanEmbedding from(Map<String, Object> map, String text) {
            Long inputTextTokenCount = (Long) map.get("inputTextTokenCount");
            List<Double> embedding = (List<Double>) map.get("embedding");
            
            return new TitanEmbedding(inputTextTokenCount, text, embedding);
        }
    }
}
