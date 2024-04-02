package apoc.ml.vectordb;

import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;


/*
TODO:
TODO - Quadrad. VECTOR DATABASE.. —> 
	—> read data
		:score and metadata, 
	Context —> yield —> lookup —> transaction.findNodes —> .. primary key..
	—> topology	information
	Neo4j Vector index integrations —> !! 
 */

public class Pinecone {
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
