package apoc.vectordb;

import apoc.result.MapResult;
import apoc.util.UrlResolver;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.util.MapUtil.map;
import static apoc.vectordb.VectorDb.getEmbeddingResultStream;
import static apoc.vectordb.VectorEmbeddingConfig.*;

public class ChromaDb {

    @Context
    public ProcedureCallContext procedureCallContext;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    // todo - create an enum Factory in case of others VectorDbs
    //  e.g.  ChromaType.from()
    public static class ChromaEmbeddingType {

        public static VectorEmbeddingConfig fromGet(Map<String, Object> config,
                                                    ProcedureCallContext procedureCallContext,
                                                    List<Object> ids) {
            
            List<String> fields = procedureCallContext.outputFields().toList();

            Map<String, Object> additionalBodies = map("ids", ids);

            return getVectorEmbeddingConfig(config, fields, additionalBodies);
        }

        public static VectorEmbeddingConfig fromQuery(Map<String, Object> config, 
                                                      ProcedureCallContext procedureCallContext,
                                                      List<Double> vector,
                                                      Map<String, Object> filter,
                                                      long limit) {
            
            List<String> fields = procedureCallContext.outputFields().toList();

            Map<String, Object> additionalBodies = map("query_embeddings", List.of(vector),
                    "where", filter,
                    "n_results", limit);

            return getVectorEmbeddingConfig(config, fields, additionalBodies);
        }
        
        private static VectorEmbeddingConfig getVectorEmbeddingConfig(Map<String, Object> config,
                                                                      List<String> fields,
                                                                      Map<String, Object> additionalBodies) {
            ArrayList<String> include = new ArrayList<>();
            if (fields.contains("metadata")) {
                include.add("metadatas");
            }
            if (fields.contains("text")) {
                include.add("documents");
            }
            if (fields.contains("embedding")) {
                include.add("embeddings");
            }
            if (fields.contains("score")) {
                include.add("distances");
            }

            additionalBodies.put("include", include);

            return new VectorEmbeddingConfig(config, Map.of(), additionalBodies);
        }
    }

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure("apoc.vectordb.chroma.create")
    @Description("apoc.vectordb.chroma.create")
    public Stream<MapResult> create(@Name("hostOrKey") String hostOrKey,
                                    @Name("name") String name,
                                    @Name("similarity") String similarity,
                                    @Name("size") String size,
                                    @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        // todo - create collection
        return null;
    }

    @Procedure("apoc.vectordb.chroma.delete")
    @Description("apoc.vectordb.chroma.delete")
    public Stream<MapResult> delete(@Name("hostOrKey") String hostOrKey, @Name("name") String name, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        // todo - delete collection
        return null;
    }

    @Procedure("apoc.vectordb.chroma.upsert")
    @Description("apoc.vectordb.chroma.upsert")
    public Stream<MapResult> upsert(@Name("hostOrKey") String hostOrKey, @Name("vectors") List<Map<String, Object>> vectors, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        // todo - upsert vectors
        return null;
    }

    @Procedure(value = "apoc.vectordb.chroma.get", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.chroma.get()")
    public Stream<VectorDbUtil.EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name("ids") List<Object> ids,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = new HashMap<>(configuration);

        String qdrantUrl = getQdrantUrl(hostOrKey);
        String endpoint = "%s/api/v1/collections/%s/get".formatted(qdrantUrl, collection);
        config.putIfAbsent(ENDPOINT_KEY, endpoint);

        VectorEmbeddingConfig apiConfig = ChromaEmbeddingType.fromGet(config, procedureCallContext, ids);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, db, tx,
                v -> getForList((Map) v).stream());
    }

    @Procedure(value = "apoc.vectordb.chroma.query", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.chroma.query()")
    public Stream<VectorDbUtil.EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                                      @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
                                                      @Name(value = "limit", defaultValue = "10") long limit,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        var config = new HashMap<>(configuration);

        String qdrantUrl = getQdrantUrl(hostOrKey);
        String endpoint = "%s/api/v1/collections/%s/query".formatted(qdrantUrl, collection);
        config.putIfAbsent(ENDPOINT_KEY, endpoint);

        VectorEmbeddingConfig apiConfig = ChromaEmbeddingType.fromQuery(config, procedureCallContext, vector, filter, limit);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, db, tx,
                v -> queryForList((Map) v).stream());
    }

    private static List<Map> queryForList(Map startMap) {
        List distances = startMap.get("distances") == null 
                ? null 
                : ((List<List>) startMap.get("distances"))
                        .get(0);
        List metadatas = startMap.get("metadatas") == null
                ? null
                : ((List<List>) startMap.get("metadatas"))
                .get(0);
        List documents = startMap.get("documents") == null
                ? null
                : ((List<List>) startMap.get("documents"))
                .get(0);
        List embeddings = startMap.get("embeddings") == null
                ? null
                : ((List<List>) startMap.get("embeddings"))
                .get(0);

        List ids = ((List<List>) startMap.get("ids")).get(0);

        return getMaps(distances, metadatas, documents, embeddings, ids);
    }

    private static List<Map> getForList(Map startMap) {
        List distances = (List) startMap.get("distances");
        List metadatas = (List) startMap.get("metadatas");
        List documents = (List) startMap.get("documents");
        List embeddings = (List) startMap.get("embeddings");

        List ids = (List) startMap.get("ids");

        return getMaps(distances, metadatas, documents, embeddings, ids);
    }

    private static List<Map> getMaps(List distances, List metadatas, List documents, List embeddings, List ids) {
        final List<Map> result = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            Map<String, Object> map = map(DEFAULT_ID, ids.get(i));
            if (CollectionUtils.isNotEmpty(distances)) {
                map.put(DEFAULT_SCORE, distances.get(i));
            }
            if (CollectionUtils.isNotEmpty(metadatas)) {
                map.put(DEFAULT_METADATA, metadatas.get(i));
            }
            if (CollectionUtils.isNotEmpty(documents)) {
                map.put(DEFAULT_TEXT, documents.get(i));
            }
            if (CollectionUtils.isNotEmpty(embeddings)) {
                map.put(DEFAULT_EMBEDDING, embeddings.get(i));
            }
            result.add(map);
        }

        return result;
    }

    protected String getQdrantUrl(String hostOrKey) {
        return new UrlResolver("http", "localhost", 6333).getUrl("qdrant", hostOrKey);
    }
}
