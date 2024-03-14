package apoc.ml;

public class VectorDatabasesTest {
    
    // todo - mettere la dimension degli index come variabile d'ambiente configurabile
    
    
    /*
    curl -X GET "https://api.pinecone.io/indexes" \
    -H "Api-Key: YOUR_API_KEY"


curl -X POST "https://YOUR_INDEX_ENDPOINT/vectors/upsert" \
  -H "Api-Key: YOUR_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "vectors": [
      {
        "id": "vec1", 
        "values": [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1],
        "metadata": {"genre": "drama"}
      },
      {
        "id": "vec2", 
        "values": [0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2],
        "metadata": {"genre": "action"}
      },
      {
        "id": "vec3", 
        "values": [0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3],
        "metadata": {"genre": "drama"}
      },
      {
        "id": "vec4", 
        "values": [0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4],
        "metadata": {"genre": "action"}
      }
    ],
    "namespace": "ns1"
  }'



curl -X POST "https://YOUR_INDEX_ENDPOINT/query" \
  -H "Api-Key: YOUR_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "namespace": "ns1",
    "vector": [0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3],
    "topK": 2,
    "includeValues": true,
    "includeMetadata": true,
    "filter": {"genre": {"$eq": "action"}}
  }'
     */
}
