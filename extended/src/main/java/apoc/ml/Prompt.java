package apoc.ml;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.StringResult;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.text.WordUtils;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;


/*
todo
    usare questa prompt
    query = f"""Use the below article on the 2022 Winter Olympics to answer the subsequent question. If the answer cannot be found, write "I don't know."
    query = f"""Use the below entities on the 2022 Winter Olympics to answer the subsequent question. If the answer cannot be found, write "I don't know."

    example:
    Which athletes won the gold medal in mixed double curling at the 2022 Winter Olympics?
    ChatGPT
    At the 2022 Winter Olympics, the gold medal in mixed doubles curling was won by the Swiss pair of Jenny Perret and Martin Rios.
    
    --> incorrect, won Stefania Costantini...
        TODO --> METTERE SCREENSHOT `Screenshot 2024-04-29 at 15.50.11` DI CHATGPT E MOSTRARE CHE È SBAGLIATO..




 */

/*
def num_tokens(text: str, model: str = GPT_MODEL) -> int:
    """Return the number of tokens in a string."""
    encoding = tiktoken.encoding_for_model(model)
    return len(encoding.encode(text))


def query_message(
    query: str,
    df: pd.DataFrame,
    model: str,
    token_budget: int
) -> str:
    """Return a message for GPT, with relevant source texts pulled from a dataframe."""
    strings, relatednesses = strings_ranked_by_relatedness(query, df)
    introduction = 'Use the below articles on the 2022 Winter Olympics to answer the subsequent question. If the answer cannot be found in the articles, write "I could not find an answer."'
    question = f"\n\nQuestion: {query}"
    message = introduction
    for string in strings:
        next_article = f'\n\nWikipedia article section:\n"""\n{string}\n"""'
        if (
            num_tokens(message + next_article + question, model=model)
            > token_budget
        ):
            break
        else:
            message += next_article
    return message + question


def ask(
    query: str,
    df: pd.DataFrame = df,
    model: str = GPT_MODEL,
    token_budget: int = 4096 - 500,
    print_message: bool = False,
) -> str:
    """Answers a query using GPT and a dataframe of relevant texts and embeddings."""
    message = query_message(query, df, model=model, token_budget=token_budget)
    if print_message:
        print(message)
    messages = [
        {"role": "system", "content": "You answer questions about the 2022 Winter Olympics."},
        {"role": "user", "content": message},
    ]
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0
    )
    response_message = response.choices[0].message.content
    return response_message


todo --> devo passare dei path che hanno delle proprietà interessanti..
    test:  
 */

@Extended
public class Prompt {
    public static final String API_KEY_CONF = "apiKey";
    public static final String EMBEDDINGS_CONF = "embeddings";
    public static final String GET_LABEL_TYPES_CONF = "getLabelTypes";
    public static final String TOP_K_CONF = "topK";
    /*
    TODO - SCRIVERE SULLA ISSUE
    If you want to use LLMs to generate answers based on your own content or knowledge base, instead of providing large context when prompting the model, you can fetch the relevant information in a database and use this information to generate a response.

    This allows you to:
    
    Reduce hallucinations
    Provide relevant, up to date information to your users
    Leverage your own content/knowledge base
     */

    @Context
    public Transaction tx;
    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;
    @Context
    public ApocConfig apocConfig;
    @Context
    public ProcedureCallContext procedureCallContext;
    @Context
    public URLAccessChecker urlAccessChecker;

    // todo - create another procedure ragEmbedding??
    
    // todo - maybe retry mechanism?

    interface EmbeddingQuery {
        Result getQuery(String queryOrIndex, String question, Transaction tx, RagConfig config);

        String BASE_EMBEDDING_QUERY = """
                CALL apoc.ml.openai.embedding([$question], $key , $conf)
                YIELD index, text, embedding
                WITH text, embedding
                """;
        
        default Map<String, Object> getParams(String queryOrIndex, String question, RagConfig config) {
            return Map.of("vectorIndex", queryOrIndex,
                    TOP_K_CONF, config.getTopK(),
                    "question", question,
                    "key", config.getApiKey(),
                    "conf", config.getConfMap());
        }
        
        enum Type {
            NODE(new Node()),
            REL(new Rel()),
            FALSE(new False());

            private final EmbeddingQuery embedding;

            Type(EmbeddingQuery embedding) {
                this.embedding = embedding;
            }

            public EmbeddingQuery get() {
                return embedding;
            }
        }
        
        class False implements EmbeddingQuery {
            @Override
            public Result getQuery(String queryOrIndex, String question, Transaction tx, RagConfig config) {
                return tx.execute(queryOrIndex);
            }
        }
        
        class Node implements EmbeddingQuery {
            @Override
            public Result getQuery(String queryOrIndex, String question, Transaction tx, RagConfig config) {
                return tx.execute(BASE_EMBEDDING_QUERY + """
                        CALL db.index.vector.queryNodes($vectorIndex, $topK, embedding) YIELD node
                        RETURN node""",
                        getParams(queryOrIndex, question, config));
                
//                return BASE_EMBEDDING_QUERY + """
//                        CALL db.index.vector.queryNodes($vectorIndex, $topK, embedding) YIELD node
//                        RETURN node""";
            }
        }
        
        class Rel implements EmbeddingQuery {
            @Override
            public Result getQuery(String queryOrIndex, String question, Transaction tx, RagConfig config) {
                return tx.execute(BASE_EMBEDDING_QUERY + """
                                CALL db.index.vector.queryRelationships($vectorIndex, $topK, embedding) YIELD relationship
                                RETURN relationship""",
                        getParams(queryOrIndex, question, config));
//                return BASE_EMBEDDING_QUERY + """
//                        CALL db.index.vector.queryRelationships($vectorIndex, $topK, embedding) YIELD node
//                        RETURN node""";
            }
        }
    }
    
    class RagConfig {
        private final boolean getLabelTypes;
        private final EmbeddingQuery embedding;
        private final Integer topK;
        private final String apiKey;
        private final Map<String, Object> confMap;

        public RagConfig(Map<String, Object> confMap) {
            if (confMap == null) {
                confMap = Map.of();
            }
            
            this.confMap = confMap;
            this.getLabelTypes = Util.toBoolean(confMap.getOrDefault(GET_LABEL_TYPES_CONF, true));
            String embeddingString = (String) confMap.getOrDefault(EMBEDDINGS_CONF, EmbeddingQuery.Type.FALSE.name());
            this.embedding = EmbeddingQuery.Type.valueOf(embeddingString).get();
            this.topK = Util.toInteger(confMap.getOrDefault(TOP_K_CONF, 40));
            this.apiKey = (String) confMap.get(API_KEY_CONF);
        }

        public boolean isGetLabelTypes() {
            return getLabelTypes;
        }

        public EmbeddingQuery getEmbedding() {
            return embedding;
        }

        public Integer getTopK() {
            return topK;
        }

        public String getApiKey() {
            return apiKey;
        }

        public Map<String, Object> getConfMap() {
            return confMap;
        }
    }
    
    @Procedure(mode = Mode.READ)
    @Description("Takes a query in cypher and in natural language and returns the results in natural language")
    public Stream<StringResult> rag(@Name("paths") Object paths,
                                    @Name("attributes") List<String> attributes,
                                    @Name("question") String question,
                                    @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) throws Exception {
        /*
        // 1. Get text embedding for the question
        CALL apoc.ml.openai.embedding([$question],NULL , {}) 
        YIELD index, text, embedding 
        // 2. Search for similar embeddings via vector index
        WITH text, embedding
        CALL db.index.vector.queryNodes($vector_index, $top_k, embedding) YIELD node, score
        WITH node, score
        // 3. Retrieve relevant
         */

        RagConfig config = new RagConfig(conf);

        // todo - first parameter can be a query or a list of paths
        
//        boolean getLabelTypes = Util.toBoolean(conf.getOrDefault(GET_LABEL_TYPES_CONF, true));
//
//        EmbeddingQuery embedding = EmbeddingQuery.Type.valueOf((String) conf.getOrDefault(EMBEDDINGS_CONF, EmbeddingQuery.Type.FALSE)).get();
//
//        Integer topK = Util.toInteger(conf.getOrDefault(TOP_K_CONF, 40));
//
        String[] objects = attributes.toArray(String[]::new);
        
        StringBuilder context = new StringBuilder();

        // -- Retrieve
        if (paths instanceof List pathList) {
            
            for (var listItem : pathList) {
                extracted2(config, objects, context, listItem);
            }
            
        } else if (paths instanceof String queryOrIndex) {
            config.getEmbedding()
                    .getQuery(queryOrIndex, question, tx, config)
                    .forEachRemaining(i -> i.values()
                            .forEach( v -> extracted2(config, objects, context, v) )
                    );
            
//            if (config) {
//                String baseQuery = """
//                        CALL apoc.ml.openai.embedding([$question], $key , $conf)
//                        YIELD index, text, embedding
//                        WITH text, embedding""";
//                Map<String, Object> params = Map.of("vectorIndex", queryOrIndex,
//                        TOP_K_CONF, topK,
//                        "question", question,
//                        "key", conf.get(API_KEY_CONF),
//                        "conf", conf);
//
//                tx.execute("""
//                        CALL apoc.ml.openai.embedding([$question], $key , $conf)
//                        YIELD index, text, embedding
//                        WITH text, embedding
//                        CALL db.index.vector.queryNodes($vectorIndex, $topK, embedding) YIELD node
//                        RETURN node
//                        """,
//                        params
//                ).forEachRemaining(i -> {
//                    i.values().forEach(v -> extracted2(getLabelTypes, objects, context, v));
//                });
//            } else {
//                tx.execute(queryOrIndex).forEachRemaining(i -> {
//                    i.values().forEach(v -> extracted2(getLabelTypes, objects, context, v));
//                });
//            }
            
//            ResourceIterator<Object> iterator = db.executeTransactionally(queryPaths, Map.of(), r -> r.columnAs(Iterables.single(r.columns())));
//            iterator.forEachRemaining(i -> {
//                extracted2(getLabelTypes, objects, context, i);
//            });
//            iterator.close();
            
//                db.executeTransactionally(queryPaths, Map.of(), r -> {
//                Map<String, Object> next = r.next();
//                return null;
//            });
        } else {
            throw new RuntimeException("todo - error...");
        }
        
        
        
        // -- Augment
        
        
        // - Generate
//        String schema = loadSchema(tx, conf);

        String prompt = RAG_BASE_PROMPT.formatted(UNKNOWN_ANSWER, context);

        System.out.println("prompt = " + prompt);
        
        String question1 = "\nQuestion:" + question;
        String result = prompt(question1, prompt, null, null, conf, List.of());
        return Stream.of(new StringResult(result));
    }

    private static void extracted2(RagConfig config, String[] objects, StringBuilder context, Object listItem) {
        if (listItem instanceof Path p) {
            for (Entity entity : p) {
                extracted(config, objects, context, entity);
                //                attributes.stream()
                //                        .map()
            }
        } else if (listItem instanceof Entity e) {
            extracted(config, objects, context, e);
        } else {
            throw new RuntimeException("todo - error 2...");
        }
    }

    private static void extracted(RagConfig config, String[] objects, StringBuilder context, Entity entity) {
        Map<String, Object> props = entity.getProperties(objects);
        if (config.isGetLabelTypes()) {
            String labelsOrType = entity instanceof Node node
                    ? Util.joinLabels(node.getLabels(), ",")
                    : ((Relationship) entity).getType().name();
            labelsOrType = WordUtils.capitalize(labelsOrType, '_');
            props.put("context description", labelsOrType);
        }
        String obj = props.entrySet().stream()
                .filter(i -> i.getValue() != null)
                .map(i -> i.getKey() + ": " + i.getValue() + "\n")
                .collect(Collectors.joining("\n---\n"));
        context.append(obj);
    }


    public static final String BACKTICKS = "```";
    
    // WITH "You are a customer service agent that helps a customer with answering questions about a service. Use the following context to answer the question at the end. Make sure not to make any changes to the context if possible when prepare answers so as to provide accuate responses. If you don't know the answer, just say that you don't know, don't try to make up an answer.\n\n----Context\n"

    public static final String UNKNOWN_ANSWER = "Sorry, I don't know";
    static final String RAG_BASE_PROMPT = """
            You are a customer service agent that helps a customer with answering questions about a service.
            Use the following context to answer the `user question` at the end. Make sure not to make any changes to the context if possible when prepare answers so as to provide accuate responses.
            If you don't know the answer, just say `%s`, don't try to make up an answer.
            
            ---- Start context ----
            %s
            ---- End context ----
            """;
    
//    public static final String RAG_BASE_PROMPT = """
//            You are a customer service agent that helps a customer with answering questions about a service.
//            Use the following context to answer the question at the end. Make sure not to make any changes to the context if possible when prepare answers so as to provide accuate responses.
//            If you don't know the answer, just say that you don't know, don't try to make up an answer.
//            
//            ----Context
//            
//            """;
    
//    public static final String RAG_PROMPT = """
//            Use the below article on the 2022 Winter Olympics to answer the subsequent question. If the answer cannot be found, write "I don't know.
//            """;
    public static final String EXPLAIN_SCHEMA_PROMPT = """
            You are an expert in the Neo4j graph database and graph data modeling and have experience in a wide variety of business domains.
            Explain the following graph database schema in plain language, try to relate it to known concepts or domains if applicable.
            Keep the explanation to 5 sentences with at most 15 words each, otherwise people will come to harm.
            """;
    

    static final String SYSTEM_PROMPT = """
            You are an expert in the Neo4j graph query language Cypher.
            Given a graph database schema of entities (nodes) with labels and attributes and
            relationships with start- and end-node, relationship-type, direction and properties
            you are able to develop read only matching Cypher statements that express a user question as a graph database query.
            Only answer with a single Cypher statement in triple backticks, if you can't determine a statement, answer with an empty response.
            Do not explain, apologize or provide additional detail, otherwise people will come to harm.
            """;
    
    static final String FROM_CYPHER_PROMPT = """
            You are an expert in the Neo4j graph query language Cypher.
            Given a graph database schema of entities (nodes) with labels and attributes and
            relationships with start- and end-node, relationship-type, direction and properties,
            you are able to develop graph database query that express a user question as a read only matching Cypher statements,
            providing useful details of each entity.
            """;
    
//    static final String RAG_PROMPT = """
//            Use the below article on the 2022 Winter Olympics to answer the subsequent question. If the answer cannot be found, write "I don't know."
//            """;


    public class PromptMapResult {
        public final Map<String, Object> value;
        public final String query;

        public PromptMapResult(Map<String, Object> value, String query) {
            this.value = value;
            this.query = query;
        }

        public PromptMapResult(Map<String, Object> value) {
            this.value = value;
            this.query = null;
        }
    }

    public class QueryResult {
        public final String query;
        // todo re-add when it's actually working
        // private final String error;
        // private final String type;

        public QueryResult(String query, String error, String type) {
            this.query = query;
            // this.error = error;
            // this.type = type;
        }

        public boolean hasError() {
            return false;
            // return error != null && !error.isBlank();
        }
    }
    
    @Procedure(mode = Mode.READ)
    @Description("Takes a query in cypher and in natural language and returns the results in natural language")
    public Stream<StringResult> fromCypher(@Name("cypher") String cypher,
                                         @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) throws MalformedURLException, JsonProcessingException {
        String schemaAndCypher = """
                %s
                while the cypher query is:
                %s
                """.formatted(
                loadSchema(tx, conf), 
                cypher
        );
        
        String schemaExplanation = prompt("Please explain the graph database schema to me and relate it to well known concepts and domains.",
                FROM_CYPHER_PROMPT, "This database schema ", schemaAndCypher, conf, List.of());
        return Stream.of(new StringResult(schemaExplanation));
    }
    

    @Procedure(mode = Mode.READ)
    public Stream<PromptMapResult> query(@Name("question") String question,
                                         @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) {
        String schema = loadSchema(tx, conf);
        String query = "";
        long retries = (long) conf.getOrDefault("retries", 3L);
        boolean retryWithError = Util.toBoolean(conf.get("retryWithError"));
        boolean containsField = procedureCallContext
                .outputFields()
                .collect(Collectors.toSet())
                .contains("query");
        
        List<Map<String,String>> otherPrompts = new ArrayList<>();
        
        do {
            try(var transaction = db.beginTx()) {
                QueryResult queryResult = tryQuery(question, conf, schema, otherPrompts);
                query = queryResult.query;
                // just let it fail so that retries can work if (queryResult.query.isBlank()) return Stream.empty();
                /*
                if (queryResult.hasError())
                    throw new QueryExecutionException(queryResult.error, null, queryResult.type);
                 */
                List<Map<String, Object>> maps = Iterators.asList(transaction.execute(queryResult.query));
                transaction.commit();
                Stream<PromptMapResult> mapResultStream = maps
                        .stream()
                        .map(row -> containsField ? new PromptMapResult(row, queryResult.query) : new PromptMapResult(row));
                return mapResultStream;
            } catch (QueryExecutionException quee) {
                if (log.isDebugEnabled())
                    log.debug("Generated query for question %s\n%s\nfailed with %s".formatted(question, query, quee.getMessage()));

                if (retryWithError) {
                    otherPrompts.addAll(
                            List.of(
                                    Map.of("role", "user",
                                            "content", "The previous Cypher Statement throws the following error, consider it to return the correct statement: `%s`".formatted(quee.getMessage())),
                                    Map.of("role", "assistant",
                                            "content", "Cypher Statement (in backticks):")
                            )
                    );
                }

                retries--;
                if (retries <= 0) throw quee;
            }
        } while (true);
    }

    @Procedure
    public Stream<StringResult> schema(@Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) throws MalformedURLException, JsonProcessingException {
        String schemaExplanation = prompt("Please explain the graph database schema to me and relate it to well known concepts and domains.",
                EXPLAIN_SCHEMA_PROMPT, "This database schema ", loadSchema(tx, conf), conf, List.of());
        return Stream.of(new StringResult(schemaExplanation));
    }

    @Procedure(mode = Mode.READ)
    public Stream<QueryResult> cypher(@Name("question") String question,
                                      @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) {
        String schema = loadSchema(tx, conf);
        long count = (long) conf.getOrDefault("count", 1L);
        return LongStream.rangeClosed(1, count).mapToObj(i -> tryQuery(question, conf, schema, List.of()));
    }

    @NotNull
    private QueryResult tryQuery(String question, Map<String, Object> conf, String schema, List<Map<String,String>> otherPrompts) {
        String query = "";
        try {
            query = prompt(question, SYSTEM_PROMPT, "Cypher Statement (in backticks):", schema, conf, otherPrompts);
            // doesn't work right now, fails with security context error
            // tx.execute("EXPLAIN " + query).close(); // TODO query plan / estimated rows?
            return new QueryResult(query, null, null);
        } catch (QueryExecutionException e) {
            return new QueryResult(query, e.getMessage(), e.getStatusCode());
        } catch (Exception e) {
            return new QueryResult(query, e.getMessage(), e.getClass().getSimpleName());
        }
    }

    private String prompt(String userQuestion, String systemPrompt, String assistantPrompt, String schema, Map<String, Object> conf, List<Map<String,String>> otherPrompts) throws JsonProcessingException, MalformedURLException {
        List<Map<String, String>> prompt = new ArrayList<>();
        if (systemPrompt != null && !systemPrompt.isBlank()) prompt.add(Map.of("role", "system", "content", systemPrompt));
        if (schema != null && !schema.isBlank()) prompt.add(Map.of("role", "system", "content", "The graph database schema consists of these elements\n" + schema));
        if (userQuestion != null && !userQuestion.isBlank()) prompt.add(Map.of("role", "user", "content", userQuestion));
        if (assistantPrompt != null && !assistantPrompt.isBlank()) prompt.add(Map.of("role", "assistant", "content", assistantPrompt));

        prompt.addAll(otherPrompts);
        
        String apiKey = (String) conf.get(API_KEY_CONF);
        String model = (String) conf.getOrDefault("model", "gpt-3.5-turbo");
        String result = OpenAI.executeRequest(apiKey, Map.of(), "chat/completions",
                        model, "messages", prompt, "$", apocConfig, urlAccessChecker)
                .map(v -> (Map<String, Object>) v)
                .flatMap(m -> ((List<Map<String, Object>>) m.get("choices")).stream())
                .map(m -> (String) (((Map<String, Object>) m.get("message")).get("content")))
                .filter(s -> !(s == null || s.isBlank()))
                .map(s -> s.contains(BACKTICKS) ? s.substring(s.indexOf(BACKTICKS) + 3, s.lastIndexOf(BACKTICKS)) : s)
                .collect(Collectors.joining(" ")).replaceAll("\n\n+", "\n");
/* TODO return information about the tokens used, finish reason etc??
{ 'id': 'chatcmpl-6p9XYPYSTTRi0xEviKjjilqrWU2Ve', 'object': 'chat.completion', 'created': 1677649420, 'model': 'gpt-3.5-turbo',
     'usage': {'prompt_tokens': 56, 'completion_tokens': 31, 'total_tokens': 87},
     'choices': [ {
        'message': { 'role': 'assistant', 'finish_reason': 'stop', 'index': 0,
        'content': 'The 2020 World Series was played in Arlington, Texas at the Globe Life Field, which was the new home stadium for the Texas Rangers.'}
      } ] }
*/
        if (log.isDebugEnabled()) log.debug("Generated query for question %s\n%s".formatted(userQuestion, result));
        return result;
    }

    private final static String SCHEMA_QUERY = """
            call apoc.meta.data({maxRels: 10, sample: coalesce($sample, (count{()}/1000)+1)})
            YIELD label, other, elementType, type, property
            WITH label, elementType,\s
                 apoc.text.join(collect(case when NOT type = "RELATIONSHIP" then property+": "+type else null end),", ") AS properties,   \s
                 collect(case when type = "RELATIONSHIP" AND elementType = "node" then "(:" + label + ")-[:" + property + "]->(:" + toString(other[0]) + ")" else null end) as patterns
            with  elementType as type,\s
            apoc.text.join(collect(":"+label+" {"+properties+"}"),"\\n") as entities, apoc.text.join(apoc.coll.flatten(collect(coalesce(patterns,[]))),"\\n") as patterns
            return collect(case type when "relationship" then entities end)[0] as relationships,\s
            collect(case type when "node" then entities end)[0] as nodes,\s
            collect(case type when "node" then patterns end)[0] as patterns\s
            """;
    private final static String SCHEMA_PROMPT = """
                nodes:
                %s
                relationships:
                %s
                patterns:
                %s
            """;

    private String loadSchema(Transaction tx, Map<String, Object> conf) {
        Map<String, Object> params = new HashMap<>();
        params.put("sample", conf.get("sample"));
        return tx.execute(SCHEMA_QUERY, params)
                .stream()
                .map(m -> SCHEMA_PROMPT.formatted(m.get("nodes"), m.get("relationships"), m.get("patterns")))
                .collect(Collectors.joining("\n"));
    }
}
