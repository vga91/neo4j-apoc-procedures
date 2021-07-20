package apoc.export.cypher;

import org.neo4j.graphdb.schema.IndexDefinition;

import java.util.List;
import java.util.Map;

public class TemplateSchema {
    private List<Map<String, Object>> indexes;
    private List<IndexDefinition>  constraints;
    private String schemaAwait;
    private String indexAwait;

    public TemplateSchema(List<Map<String, Object>> indexes, List<IndexDefinition> constraints, String schemaAwait, String indexAwait) {
        this.indexes = indexes;
        this.constraints = constraints;
        this.schemaAwait = schemaAwait;
        this.indexAwait = indexAwait;
    }

    public String getSchemaAwait() {
        return schemaAwait;
    }

    public String getIndexAwait() {
        return indexAwait;
    }

    public boolean isWithIndexes() {
        return indexes.size() > 0;
    }

    public List<Map<String, Object>> getIndexes() {
        return indexes;
    }

    public List<IndexDefinition> getConstraints() {
        return constraints;
    }

    public void setIndexes(List<Map<String, Object>> indexes) {
        this.indexes = indexes;
    }

    public void setConstraints(List<IndexDefinition> constraints) {
        this.constraints = constraints;
    }
}
