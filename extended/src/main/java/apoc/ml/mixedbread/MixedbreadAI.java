package apoc.ml.mixedbread;

import apoc.result.ObjectResult;
import apoc.util.JsonUtil;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_ML_MIXEDBREAD_URL;
import static apoc.ml.MLUtil.ERROR_NULL_INPUT;

public class MixedbreadAI {

    @Context
    public URLAccessChecker urlAccessChecker;
    

    /**
     * embedding is an Object instead of List<Double>, as with a request having `"encoding_format": [<multipleFormat>]`,
     * the result can be e.g. {... "embedding": { "float": [<floatEmbedding>], "base": <base64Embedding>,   } ...}
     * instead of e.g. {... "embedding": [<floatEmbedding>] ...}
     */
    public record EmbeddingResult(long index, String text, Object embedding) {}


    @Procedure("apoc.ml.mixedbread.custom")
    @Description("apoc.mixedbread.custom(, configuration) - returns the embeddings for a given text")
    public Stream<ObjectResult> custom(@Name("api_key") String apiKey, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return executeRequest(null, apiKey, configuration, MixedbreadHandler.Type.CUSTOM.get())
                .map(ObjectResult::new);
    }
    

    @Procedure("apoc.ml.mixedbread.embedding")
    @Description("apoc.mixedbread.mixedbread([texts], api_key, configuration) - returns the embeddings for a given text")
    public Stream<EmbeddingResult> getEmbedding(@Name("texts") List<String> texts, 
                                                @Name("api_key") String apiKey, 
                                                @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        if (texts == null) {
            throw new RuntimeException(ERROR_NULL_INPUT);
        }


        Map<Boolean, List<String>> collect = texts.stream()
                .collect(Collectors.groupingBy(Objects::nonNull));

        List<String> nonNullTexts = collect.get(true);

        Stream<Object> resultStream = executeRequest(nonNullTexts, apiKey, configuration, MixedbreadHandler.Type.EMBEDDING.get()/*, "embeddings", "text-embedding-ada-002", "input", nonNullTexts, "$.data", apocConfig, urlAccessChecker*/);
        Stream<EmbeddingResult> embeddingResultStream = resultStream
                .flatMap(v -> {
                    Map map = (Map) v;
                    return ((List<Map>) map.get("data"))
                            .stream();
                })
                .map(m -> {
                    Long index = (Long) m.get("index");
                    return new EmbeddingResult(index, nonNullTexts.get(index.intValue()), (Object) m.get("embedding"));
                });

        List<String> nullTexts = collect.getOrDefault(false, List.of());
        Stream<EmbeddingResult> nullResultStream = nullTexts.stream()
                .map(i -> {
                    // null text return index -1 to indicate that are not coming from `/embeddings` RestAPI
                    return new EmbeddingResult(-1, i, List.of());
                });
        return Stream.concat(embeddingResultStream, nullResultStream);
    }

    private Stream<Object> executeRequest(Object input, String accessToken, Map<String, Object> configuration, MixedbreadHandler handler) {
        try {

            String endpoint = handler.getEndpoint(configuration);

            var config = new HashMap<>(configuration);
            Map<String, Object> payloadMap = handler.getPayload(configuration, input);

            Map<String, Object> headers = Map.of("Content-Type", "application/json",
                    "Authorization", "Bearer " + accessToken);

            String payload = JsonUtil.OBJECT_MAPPER.writeValueAsString(payloadMap);

            return JsonUtil.loadJson(endpoint, headers, payload, "$", true, List.of(), urlAccessChecker);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
