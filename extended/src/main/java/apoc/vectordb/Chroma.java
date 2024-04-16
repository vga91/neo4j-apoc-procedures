package apoc.vectordb;

import apoc.result.MapResult;
import apoc.util.UrlResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.ml.RestAPIConfig.JSON_PATH;
import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.vectordb.VectorDb.getEmbeddingResultStream;
import static apoc.vectordb.VectorEmbeddingConfig.EMBEDDING_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.METADATA_KEY;

public class Chroma {

    @Context
    public ProcedureCallContext procedureCallContext;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    // todo - create an enum Factory in case of others VectorDbs
    //  e.g.  ChromaType.from()
    public static class ChromaEmbeddingType {

        public static VectorEmbeddingConfig fromGet(Map<String, Object> config, ProcedureCallContext procedureCallContext, List<Long> ids) {
            List<String> fields = procedureCallContext.outputFields().toList();

//            // "with_payload": <boolean> and "with_vectors": <boolean> return the metadata and vector, if true
//            // therefore is the RestAPI itself that doesn't return the data if `YIELD ` has not metadata/embedding  
//            Map additionalBodies = Map.of("with_payload", fields.contains("metadata"),
//                    "with_vectors", fields.contains("embedding"),
//                    "ids", ids);
//
//            config.putIfAbsent(EMBEDDING_KEY, "vector");
//            config.putIfAbsent(METADATA_KEY, "payload");
//            config.putIfAbsent(JSON_PATH, "result");
//
//            config.putIfAbsent(METHOD_KEY, "POST");

            return new VectorEmbeddingConfig(config, Map.of(), additionalBodies);
        }

        public static VectorEmbeddingConfig fromQuery(Map<String, Object> config, ProcedureCallContext procedureCallContext,
                                                      List<Double> vector, Map<String, Object> filter, long limit) {
            List<String> fields = procedureCallContext.outputFields().toList();

//            // "with_payload": <boolean> and "with_vectors": <boolean> return the metadata and vector, if true
//            // therefore is the RestAPI itself that doesn't return the data if `YIELD ` has not metadata/embedding  
//            Map additionalBodies = Map.of("with_payload", fields.contains("metadata"),
//                    "with_vectors", fields.contains("embedding"),
//                    "vector", vector,
//                    "filter", filter,
//                    "limit", limit);
//
//            config.putIfAbsent(EMBEDDING_KEY, "vector");
//            config.putIfAbsent(METADATA_KEY, "payload");
//            config.putIfAbsent(JSON_PATH, "result");

            return new VectorEmbeddingConfig(config, Map.of(), additionalBodies);
        }
    }

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure("apoc.vectordb.qdrant.create")
    @Description("apoc.vectordb.qdrant.create")
    public Stream<MapResult> create(@Name("hostOrKey") String hostOrKey,
                                    @Name("name") String name,
                                    @Name("similarity") String similarity,
                                    @Name("size") String size,
                                    @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        // todo - create collection
        return null;
    }

    @Procedure("apoc.vectordb.qdrant.delete")
    @Description("apoc.vectordb.qdrant.delete")
    public Stream<MapResult> delete(@Name("hostOrKey") String hostOrKey, @Name("name") String name, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        // todo - delete collection
        return null;
    }

    @Procedure("apoc.vectordb.qdrant.upsert")
    @Description("apoc.vectordb.qdrant.upsert")
    public Stream<MapResult> upsert(@Name("hostOrKey") String hostOrKey, @Name("vectors") List<Map<String, Object>> vectors, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        // todo - upsert vectors
        return null;
    }

    @Procedure(value = "apoc.vectordb.qdrant.get", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.qdrant.get()")
    public Stream<VectorDbUtil.EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name("ids") List<Long> ids,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = new HashMap<>(configuration);

        String qdrantUrl = getQdrantUrl(hostOrKey);
        String endpoint = "%s/api/v1/collections/%s/get".formatted(qdrantUrl, collection);
        config.putIfAbsent(ENDPOINT_KEY, endpoint);

        VectorEmbeddingConfig apiConfig = Qdrant.QdrantEmbeddingType.fromGet(config, procedureCallContext, ids);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, db, tx);
    }

    @Procedure(value = "apoc.vectordb.qdrant.query", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.qdrant.query()")
    public Stream<VectorDbUtil.EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                                      @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
                                                      @Name(value = "limit", defaultValue = "10") long limit,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {


        var config = new HashMap<>(configuration);

        String qdrantUrl = getQdrantUrl(hostOrKey);
        String endpoint = "%s/collections/%s/points/search".formatted(qdrantUrl, collection);
        config.putIfAbsent(ENDPOINT_KEY, endpoint);

        VectorEmbeddingConfig apiConfig = Qdrant.QdrantEmbeddingType.fromQuery(config, procedureCallContext, vector, filter, limit);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, db, tx);
    }


    protected String getQdrantUrl(String hostOrKey) {
        return new UrlResolver("http", "localhost", 6333).getUrl("qdrant", hostOrKey);
    }
}
