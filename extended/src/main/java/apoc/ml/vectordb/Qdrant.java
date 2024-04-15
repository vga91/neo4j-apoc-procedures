package apoc.ml.vectordb;

import apoc.ml.RestAPIConfig;
import apoc.util.JsonUtil;
import apoc.util.UrlResolver;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.ml.vectordb.VectorDb.getEmbeddingResultStream;
import static apoc.util.JsonUtil.OBJECT_MAPPER;

public class Qdrant {
    public static class QdrantConfig extends RestAPIConfig {

        public QdrantConfig(Map<String, Object> config) {
            super(config);
        }
    }
    @Context
    public URLAccessChecker urlAccessChecker;

//    @Procedure("apoc.vectordb.qdrant.query")
//    @Description("apoc.vectordb.qdrant.query()")
//    public Stream<VectorDb.EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
//                                                @Name("query") String filter,
////                                                         @Name("apiKey") String apiKey,
//                                                @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
//        var config = new HashMap<>(configuration);
//           String endpoint = "%s/collections/%s/points/%s".formatted(qdrantUrl, collection, id);
//        config.putIfAbsent(ENDPOINT_KEY, getQdrantUrl(hostOrKey));// + "test_collection/points/search");
//
//        QdrantConfig apiConfig = new QdrantConfig(config);
//        Stream<Object> resultStream = executeRequest(/*apiKey, */apiConfig);
//        return resultStream
//                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
//                .map(m -> {
//                    System.out.println("m = " + m);
//                    return new VectorDb.EmbeddingResult(0, (List<Double>) m.get("embedding"), 0.2);
//                });
//    }


    // -- todo: https://qdrant.tech/documentation/concepts/points/#retrieve-points
    
    @Context
    public ProcedureCallContext procedureCallContext;
    
    // todo - richiamare la base procs
    @Procedure("apoc.vectordb.qdrant.query")
    @Description("apoc.vectordb.qdrant.query()")
    public Stream<VectorDb.EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                @Name("collection") String collection,
                                                @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                                @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
                                                @Name(value = "limit", defaultValue = "10") long limit,
//                                                         @Name("apiKey") String apiKey,
                                                @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = new HashMap<>(configuration);
        
        String qdrantUrl = getQdrantUrl(hostOrKey);
        String endpoint = "%s/collections/%s/points/search".formatted(qdrantUrl, collection);
        config.putIfAbsent(ENDPOINT_KEY, endpoint);

        List<String> fields = procedureCallContext.outputFields().toList();
        // todo - handle stuff..
//        fields.contains("")
        
        VectorDb.VectorEmbeddingConfig apiConfig = VectorDb.QdrantType.from(config, procedureCallContext, vector, filter, limit);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker);
        
//        Stream<Object> resultStream = executeRequest(/*apiKey, */apiConfig);
//        return resultStream
//                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
//                .map(m -> {
//                    System.out.println("m = " + m);
//                    return new VectorDb.EmbeddingResult(0, (List<Double>) m.get("embedding"), 0.2);
//                });
    }

    private Stream<Object> executeRequest(RestAPIConfig apiConfig) throws JsonProcessingException {
        Map<String, Object> headers = new HashMap<>(apiConfig.getHeaders());
        
        String bodyString = apiConfig.getBody() == null
                ? ""
                : OBJECT_MAPPER.writeValueAsString(apiConfig.getBody());
        
        return JsonUtil.loadJson(apiConfig.getEndpoint(), headers, bodyString, apiConfig.getJsonPath(), true, List.of(), urlAccessChecker);
    }

    protected String getQdrantUrl(String hostOrKey) {
        return new UrlResolver("http", "localhost", 6333).getUrl("qdrant", hostOrKey);
    }
}
