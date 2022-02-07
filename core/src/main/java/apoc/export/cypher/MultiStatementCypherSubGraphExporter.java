package apoc.export.cypher;

import apoc.export.cypher.formatter.CypherFormatter;
import apoc.export.cypher.formatter.CypherFormatterUtils;
import apoc.export.cypher.formatter.TemplateCypherHelpers;
import apoc.export.util.ExportConfig;
import apoc.export.util.Reporter;
import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.github.jknack.handlebars.helper.ConditionalHelpers;
import com.github.jknack.handlebars.helper.StringHelpers;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexDefinition;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;



/*
 * Idea is to lookup nodes for relationships via a unique index
 * either one inherent to the original node, or a artificial one that indexes the original node-id
 * and which is removed after the import.
 * <p>
 * Outputs indexes and constraints at the beginning as their own transactions
 */
public class MultiStatementCypherSubGraphExporter {
    private static final String SCHEMA_FILE = "schema";
    private static final String CLEANUP_FILE = "cleanup";
    private static final String REL_FILE = "relationships";
    private static final String NODES_FILE = "nodes";

    /*private enum IndexType {
        NODE_LABEL_PROPERTY("node_label_property"),
        NODE_UNIQUE_PROPERTY("node_unique_property"),
        REL_TYPE_PROPERTY("relationship_type_property"),
        NODE_FULLTEXT("node_fulltext"),
        RELATIONSHIP_FULLTEXT("relationship_fulltext");

        private final String typeName;

        IndexType(String typeName) {
            this.typeName = typeName;
        }

        static IndexType from(String type, String entityType, String uniqueness) {
            if (uniqueness.equals("UNIQUE") && entityType.equals("NODE")) {
                return NODE_UNIQUE_PROPERTY
            }

            return Stream.of(IndexType.values()).filter(type -> type.typeName().equals(stringType)).findFirst().orElseThrow();
        }

        public String typeName() {
            return typeName;
        }
    }*/

    private final SubGraph graph;
    private static final Map<String, Set<String>> uniqueConstraints = new HashMap<>();
    private Set<String> indexNames        = new LinkedHashSet<>();
    private Set<String> indexedProperties = new LinkedHashSet<>();

    private CypherFormatter cypherFormat;
    private GraphDatabaseService db;
    private TemplateCypher templateCypher;

    public MultiStatementCypherSubGraphExporter(SubGraph graph, ExportConfig config, GraphDatabaseService db) {
        this.graph = graph;
        gatherUniqueConstraints();
        this.templateCypher = new TemplateCypher(config, uniqueConstraints, indexNames, indexedProperties);
        this.cypherFormat = config.getCypherFormat().getFormatter();
        this.db = db;
    }

    /**
     * Given a full path file name like <code>/tmp/myexport.cypher</code>,
     * when <code>ExportConfig#separateFiles() == true</code>,
     * this method will create the following files:
     * <ul>
     * <li>/tmp/myexport.nodes.cypher</li>
     * <li>/tmp/myexport.schema.cypher</li>
     * <li>/tmp/myexport.relationships.cypher</li>
     * <li>/tmp/myexport.cleanup.cypher</li>
     * </ul>
     * Otherwise all kernelTransaction will be saved in the original file.
     * @param config
     * @param reporter
     * @param cypherFileManager
     */
    public void export(ExportConfig config, Reporter reporter, ExportFileManager cypherFileManager) {
        try {
            Handlebars handlebars = getHandlebars();
            Template templateHandlebarsNodes = handlebars.compile(NODES_FILE);
            Template templateHandlebarsRels = handlebars.compile(REL_FILE);
            Template templateHandlebarsCleanup = handlebars.compile(CLEANUP_FILE);
            Template templateHandlebarsSchema = handlebars.compile(SCHEMA_FILE);

            templateCypher.setReporter(reporter);

            ExportConfig.OptimizationType useOptimizations = config.getOptimizationType();

            PrintWriter schemaWriter = cypherFileManager.getPrintWriter(SCHEMA_FILE);
            PrintWriter nodesWriter = cypherFileManager.getPrintWriter(NODES_FILE);
            PrintWriter relationshipsWriter = cypherFileManager.getPrintWriter(REL_FILE);
            PrintWriter cleanupWriter = cypherFileManager.getPrintWriter(CLEANUP_FILE);

            switch (useOptimizations) {
                case NONE:
                    templateCypher.setNodes(graph.getNodes());
                    exportSchema();
                    templateCypher.setRelationships(graph.getRelationships());

                    templateHandlebarsNodes.apply(templateCypher, nodesWriter);
                    templateHandlebarsSchema.apply(templateCypher, schemaWriter);
                    templateHandlebarsRels.apply(templateCypher, relationshipsWriter);
                    break;
                default:
                    templateCypher.incrementArtificialUniques(countArtificialUniques(graph.getNodes()));

                    exportSchema();
                    exportNodesUnwindBatch();
                    exportRelationshipsUnwindBatch();
                    
                    templateHandlebarsSchema.apply(templateCypher, schemaWriter);
                    templateHandlebarsNodes.apply(templateCypher, nodesWriter);
                    templateHandlebarsRels.apply(templateCypher, relationshipsWriter);
                    
                    reporter.update(templateCypher.getNodeCount().get(), templateCypher.getRelCount().get(), templateCypher.getPropertyCount().get());
                    break;
            }
            
            if (cypherFileManager.separatedFiles()) {
                nodesWriter.close();
                schemaWriter.close();
                relationshipsWriter.close();
            }

            templateHandlebarsCleanup.apply(templateCypher, cleanupWriter);
            cleanupWriter.close();
            reporter.done();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void exportOnlySchema(ExportFileManager cypherFileManager) {
        try {
            Handlebars handlebars = new Handlebars()
                    .prettyPrint(true)
                    .registerHelpers(ConditionalHelpers.class)
                    .registerHelpers(StringHelpers.class)
                    .registerHelpers(TemplateCypherHelpers.class);
            Template templateHandlebars = handlebars.compile(SCHEMA_FILE);

            PrintWriter schemaWriter = cypherFileManager.getPrintWriter(SCHEMA_FILE);
            exportSchema();
            templateHandlebars.apply(templateCypher, schemaWriter);
            schemaWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Handlebars getHandlebars() {
        return new Handlebars()
                .prettyPrint(false)
                .registerHelpers(StringHelpers.class)
                .registerHelpers(ConditionalHelpers.class)
                .registerHelpers(TemplateCypherHelpers.class);
    }

    // ---- Nodes ----

    private void exportNodesUnwindBatch() {
        if (graph.getNodes().iterator().hasNext()) {
            cypherFormat.groupNodes(graph.getNodes(), uniqueConstraints, db, templateCypher);
        }
    }

    // ---- Relationships ----

    private void exportRelationshipsUnwindBatch() {
        if (graph.getRelationships().iterator().hasNext()) {
            cypherFormat.groupRelationships(graph.getRelationships(), uniqueConstraints, db, templateCypher);
        }
    }

    // ---- Schema ----

    private void exportSchema() {
        final TemplateSchema templateSchema = templateCypher.getTemplateSchema();
        templateSchema.setIndexes(exportIndexes());
        templateSchema.setConstraints(exportConstraints());
    }

    private List<Map<String, Object>> exportIndexes() {
        return db.executeTransactionally("CALL db.indexes()", Collections.emptyMap(), result -> result.stream()
                .map(map -> {
                    List<String> tokenNames = (List<String>) map.get("labelsOrTypes");
                    boolean inGraph = tokensInGraph(tokenNames);
                    if (!inGraph) {
                        return null;
                    }

                    if ("UNIQUE".equals(map.get("uniqueness"))) {
                        return null;  // delegate to the constraint creation
                    }

                    return map;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
    }

    private boolean tokensInGraph(List<String> tokens) {
        return StreamSupport.stream(graph.getIndexes().spliterator(), false)
                .anyMatch(indexDefinition -> {
                    if (indexDefinition.isRelationshipIndex()) {
                        List<String> typeNames = StreamSupport.stream(indexDefinition.getRelationshipTypes().spliterator(), false)
                                .map(RelationshipType::name)
                                .collect(Collectors.toList());
                        return typeNames.containsAll(tokens);
                    } else {
                        List<String> labelNames = StreamSupport.stream(indexDefinition.getLabels().spliterator(), false)
                                .map(Label::name)
                                .collect(Collectors.toList());
                        return labelNames.containsAll(tokens);
                    }
                });
    }

    public static List<Label> toLabels(List<String> tokenNames) {
        return tokenNames.stream()
                .map(Label::label)
                .collect(Collectors.toList());
    }

    private List<IndexDefinition> exportConstraints() {
        return StreamSupport.stream(graph.getIndexes().spliterator(), false)
                .filter(index -> index.isConstraintIndex())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    // ---- Common ----

    private void gatherUniqueConstraints() {
        for (IndexDefinition indexDefinition : graph.getIndexes()) {
            Set<String> label = StreamSupport.stream(indexDefinition.getLabels().spliterator(), false)
                    .map(Label::name)
                    .collect(Collectors.toSet());
            Set<String> props = StreamSupport
                    .stream(indexDefinition.getPropertyKeys().spliterator(), false)
                    .collect(Collectors.toSet());
            indexNames.add(indexDefinition.getName());
            indexedProperties.addAll(props);
            if (indexDefinition.isConstraintIndex()) { // we use the constraint that have few properties
                uniqueConstraints.compute(String.join(":", label), (k, v) ->  v == null || v.size() > props.size() ? props : v);
            }
        }
    }

    public static long countArtificialUniques(Node node) {
        long artificialUniques = 0;
        artificialUniques = getArtificialUniques(node, artificialUniques);
        return artificialUniques;
    }

    private static long countArtificialUniques(Iterable<Node> n) {
        long artificialUniques = 0;
        for (Node node : n) {
            artificialUniques = getArtificialUniques(node, artificialUniques);
        }
        return artificialUniques;
    }

    private static long getArtificialUniques(Node node, long artificialUniques) {
        Iterator<Label> labels = node.getLabels().iterator();
        boolean uniqueFound = false;
        while (labels.hasNext()) {
            Label next = labels.next();
            String labelName = next.name();
            uniqueFound = CypherFormatterUtils.isUniqueLabelFound(node, uniqueConstraints, labelName);

        }
        if (!uniqueFound) {
            artificialUniques++;
        }
        return artificialUniques;
    }
}
