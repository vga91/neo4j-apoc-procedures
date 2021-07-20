package apoc.export.cypher;

import apoc.export.cypher.formatter.CypherFormatterUtils;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportFormat;
import apoc.export.util.Reporter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class TemplateCypher extends CypherFormatterUtils {
    private final Map<String, Set<String>> uniqueConstraints;
    private final Set<String> indexNames;
    private final Set<String> indexedProperties;
    private final ExportConfig exportConfig;
    private final TemplateSchema templateSchema;

    private long artificialUniques;
    
    private Reporter reporter;
    private Iterable<Node> nodes;
    private Iterable<Relationship> relationships; 
    
    private Map<Map<String, Object>, List<Relationship>> groupedRelationships;
    private Map<Map.Entry<Set<String>, Set<String>>, List<Node>> groupedNodes;
    
    private final String begin;
    private final String commit;
    
    private final AtomicInteger nodeCount = new AtomicInteger(0);
    private final AtomicInteger relCount = new AtomicInteger(0);
    private final AtomicInteger relBatchCount = new AtomicInteger(0);
    private final AtomicInteger nodeBatchCount = new AtomicInteger(0);
    private final AtomicInteger unwindCount = new AtomicInteger(0);
    private final AtomicInteger propertyCount = new AtomicInteger(0);

    public TemplateCypher(ExportConfig exportConfig, Map<String, Set<String>> uniqueConstraints, Set<String> indexNames, Set<String> indexedProperties) {
        final ExportFormat format = exportConfig.getFormat();
        this.uniqueConstraints = uniqueConstraints;
        this.exportConfig = exportConfig;
        this.indexNames = indexNames;
        this.indexedProperties = indexedProperties;
        this.begin = format.begin();
        this.commit = format.commit();

        this.templateSchema = new TemplateSchema(new ArrayList<>(), new ArrayList<>(), format.schemaAwait(), format.indexAwait(exportConfig.getAwaitForIndexes()));
    }

    public boolean isRelBatchMatch() {
        return relBatchCount.get() % exportConfig.getBatchSize() == 0;
    }

    public boolean isNodeBatchMatch() {
        return nodeBatchCount.get() % exportConfig.getBatchSize() == 0;
    }

    public Map<Map.Entry<Set<String>, Set<String>>, List<Node>> getGroupedNodes() {
        return groupedNodes;
    }

    public Map<Map<String, Object>, List<Relationship>> getGroupedRelationships() {
        return groupedRelationships;
    }

    public AtomicInteger getPropertyCount() {
        return propertyCount;
    }

    public AtomicInteger getRelBatchCount() {
        return relBatchCount;
    }

    public AtomicInteger getNodeBatchCount() {
        return nodeBatchCount;
    }

    public AtomicInteger getUnwindCount() {
        return unwindCount;
    }

    public AtomicInteger getRelCount() {
        return relCount;
    }

    public AtomicInteger getNodeCount() {
        return nodeCount;
    }

    public Reporter getReporter() {
        return reporter;
    }

    public String getBegin() {
        return begin;
    }

    public String getCommit() {
        return commit;
    }

    public Iterable<Node> getNodes() {
        return nodes;
    }

    public Iterable<Relationship> getRelationships() {
        return relationships;
    }

    public ExportConfig getExportConfig() {
        return exportConfig;
    }

    public long getArtificialUniques() {
        return artificialUniques;
    }
    public void setReporter(Reporter reporter) {
        this.reporter = reporter;
    }

    public void setGroupedNodes(Map<Map.Entry<Set<String>, Set<String>>, List<Node>> groupedNodes) {
        this.groupedNodes = groupedNodes;
    }

    public void incrementNodeCount(int nodeCount) {
        this.nodeCount.addAndGet(nodeCount);
    }

    public void incrementRelCount(int relCount) {
        this.relCount.addAndGet(relCount);
    }

    public void incrementArtificialUniques(long artificialUniques) {
        this.artificialUniques += artificialUniques;
    }


    public void setNodes(Iterable<Node> nodes) {
        this.nodes = nodes;
    }

    public void setRelationships(Iterable<Relationship> relationships) {
        this.relationships = relationships;
    }

    public Map<String, Set<String>> getUniqueConstraints() {
        return uniqueConstraints;
    }

    public Set<String> getIndexNames() {
        return indexNames;
    }

    public Set<String> getIndexedProperties() {
        return indexedProperties;
    }

    public TemplateSchema getTemplateSchema() {
        return templateSchema;
    }

    public void setGroupedRelationships(Map<Map<String, Object>, List<Relationship>> groupedRelationships) {
        this.groupedRelationships = groupedRelationships;
    }
}
