package apoc.ml.vectordb;

import apoc.ApocConfig;
//import apoc.ml.OpenAIRequestHandler;
import apoc.ml.RestAPIConfig;
import apoc.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_AWS_KEY_ID;
import static apoc.ExtendedApocConfig.APOC_ML_OPENAI_TYPE;
import static apoc.ExtendedApocConfig.APOC_OPENAI_KEY;
import static apoc.ExtendedApocConfig.APOC_PINECONE_KEY;
import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.util.JsonUtil.OBJECT_MAPPER;


/*
TODO:
TODO - Quadrad. VECTOR DATABASE.. —> vedere se funziona più o meno uguale
	—> read data
		:score and metadata, 
	Context —> yield —> lookup —> transaction.findNodes —> .. primary key..
	—> topology	information
	Neo4j Vector index integrations —> !! 
 */


// todo - mettere in un package vectordb??
public class Pinecone {
    public static class PineconeConfig extends RestAPIConfig {
        public static final String APIKEY_CONF_KEY = "apiKey";
        
//        private final String apiKey;

        public static PineconeConfig from(Map<String, Object> config, String apiKey) {
            apiKey = apiKey == null ? apocConfig().getString(APOC_PINECONE_KEY, null) : apiKey;
            if (StringUtils.isBlank(apiKey)) {
                throw new IllegalArgumentException("API Key must not be empty");
            }
            
            config.putIfAbsent("Api-Key", apiKey);
            return new PineconeConfig(config);
        }
        
        
        protected PineconeConfig(Map<String, Object> config) {
            super(config);
//            this.getHeaders().putIfAbsent() 
//            this.apiKey = key;
        }

        @Override
        public String getEndpoint() {
            return super.getEndpoint();
        }

//        public String getApiKey() {
//            return apiKey;
//        }
    }
    
    @Procedure("apoc.vectordb.pinecone.get")
    @Description("apoc.vectordb.pinecone.get()")
    public Stream<VectorDb.EmbeddingResult> getEmbedding(@Name("indexHost") String indexHost,
                                                         @Name("apiKey") String apiKey,
                                                         @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
    /*
    { "object": "list",
      "data": [
        {
          "object": "embedding",
          "embedding": [ 0.0023064255, -0.009327292, .... (1536 floats total for ada-002) -0.0028842222 ],
          "index": 0
        }
      ],
      "model": "text-embedding-ada-002",
      "usage": { "prompt_tokens": 8, "total_tokens": 8 } }
    */

        var config = new HashMap<>(configuration);
        config.putIfAbsent(ENDPOINT_KEY, "https://%s/query".formatted(indexHost));
        RestAPIConfig apiConfig = new PineconeConfig(config);

        Stream<Object> resultStream = executeRequest(apiKey, apiConfig);
//        Stream<Object> resultStream = executeRequest(apiKey, configuration, "embeddings", "text-embedding-ada-002", "input", texts, "$.data", apocConfig, urlAccessChecker);
        return resultStream
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(m -> {
                    System.out.println("m = " + m);
                    return new VectorDb.EmbeddingResult(0, (List<Double>) m.get("embedding"), 0.2);
                });
    }

    private Stream<Object> executeRequest(String apiKey, RestAPIConfig apiConfig/*, String path, String model, String key, Object inputs, String jsonPath, ApocConfig apocConfig, URLAccessChecker urlAccessChecker*/) throws JsonProcessingException, MalformedURLException {

        
        
//        apiKey = (String) configuration.getOrDefault(APOC_PINECONE_KEY, apocConfig.getString(APOC_PINECONE_KEY, apiKey));
//        if (apiKey == null || apiKey.isBlank()) {
//            throw new IllegalArgumentException("API Key must not be empty");
//        }

//        String apiTypeString = (String) configuration.getOrDefault(API_TYPE_CONF_KEY,
//                apocConfig.getString(APOC_ML_OPENAI_TYPE, OpenAIRequestHandler.Type.OPENAI.name())
//        );
//        OpenAIRequestHandler.Type type = OpenAIRequestHandler.Type.valueOf(apiTypeString.toUpperCase(Locale.ENGLISH));

//        var config = new HashMap<>(configuration);
        // we remove these keys from config, since the json payload is calculated starting from the config map
//        Stream.of(ENDPOINT_CONF_KEY, API_TYPE_CONF_KEY, API_VERSION_CONF_KEY, APIKEY_CONF_KEY).forEach(config::remove);

//        switch (type) {
//            case HUGGINGFACE -> {
//                config.putIfAbsent("inputs", inputs);
//                jsonPath = "$[0]";
//            }
//            default -> {
//                config.putIfAbsent(MODEL_CONF_KEY, model);
//                config.put(key, inputs);
//            }
//        }

//        OpenAIRequestHandler apiType = type.get();

//        jsonPath = (String) configuration.getOrDefault(JSON_PATH_CONF_KEY, jsonPath);
//        path = (String) configuration.getOrDefault(PATH_CONF_KEY, path);

//        String payload = JsonUtil.OBJECT_MAPPER.writeValueAsString(config);

//        String bodyString = OBJECT_MAPPER.writeValueAsString(apiConfig.getBody());
        String bodyString = OBJECT_MAPPER.writeValueAsString(apiConfig.getBody());

        Map<String, Object> headers = new HashMap<>(apiConfig.getHeaders());
        headers.putIfAbsent("Api-Key", apiKey);
        return JsonUtil.loadJson(apiConfig.getEndpoint(), headers, bodyString, apiConfig.getJsonPath(), true, List.of(), urlAccessChecker);
    }

    // todo - in common class
//    private static Stream<Object> getObjectStream(RestAPIConfig apiConfig, URLAccessChecker urlAccessChecker) throws JsonProcessingException {
//        String bodyString = OBJECT_MAPPER.writeValueAsString(apiConfig.getBody());
//        return JsonUtil.loadJson(apiConfig.getEndpoint(), apiConfig.getHeaders(), bodyString, apiConfig.getJsonPath(), true, List.of(), urlAccessChecker);
//    }
    
    /*

curl -s -X POST "https://api.pinecone.io/indexes" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -H "Api-Key: $PINECONE_API_KEY" \
  -d '{
         "name": "quickstart",
         "dimension": 1536,
         "metric": "cosine",
         "spec": {
            "serverless": {
               "cloud": "aws",
               "region": "us-west-2"
            }
         }
      }'


# The `POST` requests below uses the unique endpoint for an index.
# See https://docs.pinecone.io/guides/data/get-an-index-endpoint for details.
PINECONE_API_KEY="YOUR_API_KEY"
INDEX_HOST="INDEX_HOST"

curl -X POST "https://$INDEX_HOST/vectors/upsert" \
  -H "Api-Key: $PINECONE_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "vectors": [
      {
        "id": "vec1", 
        "values": [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1]
      },
      {
        "id": "vec2", 
        "values": [0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2]
      },
      {
        "id": "vec3", 
        "values": [0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3]
      },
      {
        "id": "vec4", 
        "values": [0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4]
      }
    ],
    "namespace": "ns1"
  }'

curl -X POST "https://$INDEX_HOST/vectors/upsert" \
  -H "Api-Key: $PINECONE_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "vectors": [
      {
        "id": "vec5", 
        "values": [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]
      },
      {
        "id": "vec6", 
        "values": [0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6]
      },
      {
        "id": "vec7", 
        "values": [0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7]
      },
      {
        "id": "vec8", 
        "values": [0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8]
      }
    ],
    "namespace": "ns2"
  }'



# The `POST` request below uses the unique endpoint for an index.
# See https://docs.pinecone.io/guides/data/get-an-index-endpoint for details.
PINECONE_API_KEY="YOUR_API_KEY"
INDEX_HOST="INDEX_HOST"

curl -X POST "https://$INDEX_HOST/describe_index_stats" \
  -H "Api-Key: $PINECONE_API_KEY" \

# Output:
# {
#   "namespaces": {
#     "ns1": {
#       "vectorCount": 4
#     },
#     "ns2": {
#       "vectorCount": 4
#     }
#   },
#   "dimension": 8,
#   "indexFullness": 0.00008,
#   "totalVectorCount": 8
# }




# The `POST` requests below uses the unique endpoint for an index.
# See https://docs.pinecone.io/guides/data/get-an-index-endpoint for details.
PINECONE_API_KEY="YOUR_API_KEY"
INDEX_HOST="INDEX_HOST"

curl -X POST "https://$INDEX_HOST/query" \
  -H "Api-Key: $PINECONE_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "namespace": "ns1",
    "vector": [0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3],
    "topK": 3,
    "includeValues": true
  }'

curl -X POST "https://$INDEX_HOST/query" \
 \
  -H "Api-Key: $PINECONE_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "namespace": "ns2",
    "vector": [0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7],
    "topK": 3,
    "includeValues": true
  }'
# Output:
# {
#   "matches":[
#     {
#       "id": "vec3",
#       "score": 0,
#       "values": [0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3]
#     },
#     {
#       "id": "vec2",
#       "score": 0.0800000429,
#       "values": [0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2]
#     },
#     {
#       "id": "vec4",
#       "score": 0.0799999237,
#       "values": [0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4]
#     }
#   ],
#   "namespace": "ns1",
#   "usage": {"read_units": 6}
# }
# {
#   "matches": [
#     {
#       "id": "vec7",
#       "score": 0,
#       "values": [0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7]
#     },
#     {
#       "id": "vec6",
#       "score": 0.0799999237,
#       "values": [0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6]
#     },
#     {
#       "id": "vec8",
#       "score": 0.0799999237,
#       "values": [0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8]
#     }
#   ],
#   "namespace": "ns2",
#   "usage": {"read_units": 6}
# }



PINECONE_API_KEY="YOUR_API_KEY"

curl -s -X -v DELETE "https://api.pinecone.io/indexes/quickstart" \
   -H "Accept: application/json" \
   -H "Api-Key: $PINECONE_API_KEY"

     */

    @Context
    public URLAccessChecker urlAccessChecker;
    
    
    /*
    TODO: fare delle api con chroma e pinecone con la stessa firma?
    - Data
    - Upsert data
    - Query data
    - Fetch data
    - Update data
    - Delete data
    - List record IDs
    - Get an index endpoint
     */
    
    // todo - embeddingResult with metadata, id, score...
    
    
    //
    /*
    curl --request GET \
     --url 'https://apoc-test-index-ilx67g5.svc.gcp-starter.pinecone.io/vectors/fetch?ids=vec1&namespace=ns1' \
     --header 'accept: application/json' -H "Api-Key: <Api-Key>"
     */
    
    /*
    {
  "vectors": {
    "vec2": {
      "id": "vec2",
      "values": [
        0.2,
        0.2,
        0.2,
        0.2,
        0.2,
        0.2,
        0.2,
        0.2
      ],
      "metadata": {
        "genre": "action"
      }
    },
    "vec1": {
      "id": "vec1",
      "values": [
        0.1,
        0.1,
        0.1,
        0.1,
        0.1,
        0.1,
        0.1,
        0.1
      ],
      "metadata": {
        "genre": "drama"
      }
    }
  },
  "namespace": "ns1",
  "usage": {
    "readUnits": 1
  }
}
     */
}
