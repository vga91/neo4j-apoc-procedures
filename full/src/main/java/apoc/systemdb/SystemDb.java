package apoc.systemdb;

import apoc.ApocConfig;
import apoc.Description;
import apoc.Extended;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.custom.CypherProceduresHandler;
import apoc.export.cypher.ExportFileManager;
import apoc.export.cypher.FileManagerFactory;
import apoc.result.RowResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.JsonUtil.OBJECT_MAPPER;


@Extended
public class SystemDb {

    @Context
    public ApocConfig apocConfig;

    @Context
    public SecurityContext securityContext;

    @Context
    public ProcedureCallContext callContext;
    
    @Context
    public GraphDatabaseService db;

    public static class NodesAndRelationshipsResult {
        public List<Node> nodes;
        public List<Relationship> relationships;

        public NodesAndRelationshipsResult(List<Node> nodes, List<Relationship> relationships) {
            this.nodes = nodes;
            this.relationships = relationships;
        }
    }
    
    @Procedure(name = "apoc.systemdb.export.metadata")
    @Description("apoc.systemdb.export.metadata($conf) - export the apoc feature saved in system db (that is: customProcedures, triggers, uuids, and dvCatalogs) in multiple files called <FILE_NAME>.<FEATURE_NAME>.<DB_NAME>.cypher")
    public void metadata(@Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        final SystemDbConfig conf = new SystemDbConfig(config);
        final String fileName = conf.getFileName();
        apocConfig.checkWriteAllowed(null, fileName);
        
        Map<String, List<String>> exportApoc = new HashMap<>();
        
        withSystemDbTransaction(tx -> {
            tx.getAllNodes()
                    .forEach(node -> node.getLabels().forEach(label -> {
                        try {
                            final SystemLabels sysLabel = SystemLabels.valueOf(label.name());
                            String statement;
                            switch (sysLabel) {
                                case Procedure:
                                    statement = getFormatFromCustom(node, true);
                                    addToExportList(exportApoc, conf, SystemDbConfig.CUSTOM_PROCEDURES, statement, node);
                                    break;
                                case Function:
                                    statement = getFormatFromCustom(node, false);
                                    addToExportList(exportApoc, conf, SystemDbConfig.CUSTOM_PROCEDURES, statement, node);
                                    break;
                                case ApocTrigger:
                                    final String name = (String) node.getProperty(SystemPropertyKeys.name.name());
                                    final String query = (String) node.getProperty(SystemPropertyKeys.statement.name());
                                    final String selector = removeQuotesFromKey((String) node.getProperty(SystemPropertyKeys.selector.name()));
                                    final String params = removeQuotesFromKey((String) node.getProperty(SystemPropertyKeys.params.name()));
                                    statement = String.format("CALL apoc.trigger.add('%s', '%s', %s,{params: %s})", name, query, selector, params);
                                    addToExportList(exportApoc, conf, SystemDbConfig.TRIGGERS, statement, node);
                                    if ((boolean) node.getProperty(SystemPropertyKeys.paused.name())) {
                                        statement = String.format("CALL apoc.trigger.pause('%s')", name);
                                        addToExportList(exportApoc, conf, SystemDbConfig.TRIGGERS, statement, node);
                                    }
                                    break;
                                case ApocUuid:
                                    Map<String, Object> map = new HashMap<>();
                                    final String labelName = (String) node.getProperty(SystemPropertyKeys.label.name());
                                    final String property = (String) node.getProperty(SystemPropertyKeys.propertyName.name());
                                    map.put("uuidProperty", property);
                                    map.put("addToSetLabels", node.getProperty(SystemPropertyKeys.addToSetLabel.name(), null));
                                    final String uuidConfig = OBJECT_MAPPER.disable(JsonGenerator.Feature.QUOTE_FIELD_NAMES).writeValueAsString(map);
                                    // add constraint - TODO: might be worth add config to export or not this file
                                    statement = String.format("CREATE CONSTRAINT IF NOT EXISTS ON (n:%s) ASSERT n.%s IS UNIQUE", labelName, property);
                                    addToExportList(exportApoc, conf, SystemDbConfig.UUIDS, statement, node, ".schema");

                                    statement = String.format("CALL apoc.uuid.install('%s', %s) YIELD label RETURN label", labelName, uuidConfig);
                                    addToExportList(exportApoc, conf, SystemDbConfig.UUIDS, statement, node);
                                    break;
                                case DataVirtualizationCatalog:
                                    final String dvName = (String) node.getProperty(SystemPropertyKeys.name.name());
                                    final String data = removeQuotesFromKey((String) node.getProperty(SystemPropertyKeys.data.name()));
                                    statement = String.format("CALL apoc.dv.catalog.add('%s', %s)", dvName, data);
                                    addToExportList(exportApoc, conf, SystemDbConfig.DV_CATALOGS, statement, node);
                            }
                        } catch (IllegalArgumentException ignored) {
                            // ignore SystemLabels.valueOf(..) errors
                        } 
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }));
            return null;
        });
        
        ExportFileManager cypherFileManager = FileManagerFactory.createFileManager(fileName + ".cypher", true);
        
        exportApoc.forEach((key, feature) -> {
            try(PrintWriter writer = cypherFileManager.getPrintWriter(key)) {
                feature.forEach(item -> writer.write(item + ";\n"));
            }
        });
    }
    
    private String removeQuotesFromKey(String json) {
        return json.replaceAll("\"([^\"]+)\":", "$1:");
    }

    private String getFormatFromCustom(Node node, boolean isProcedure) {
        final String inputs = getSignature(node, SystemPropertyKeys.inputs.name());

        final String outputName = SystemPropertyKeys.output.name();
        final String outputs = node.hasProperty(outputName) 
                ? (String) node.getProperty(outputName)
                : getSignature(node, SystemPropertyKeys.outputs.name());

        final String formatStatement = isProcedure 
                ? "CALL apoc.custom.declareProcedure('%s(%s) :: (%s)', '%s' , '%s', '%s')" 
                : "CALL apoc.custom.declareFunction('%s(%s) :: (%s)', '%s' , %s, '%s')";
        
        return String.format(formatStatement,
                node.getProperty(SystemPropertyKeys.name.name()), inputs, outputs,
                node.getProperty(SystemPropertyKeys.statement.name()),
                node.getProperty(isProcedure ? SystemPropertyKeys.mode.name() : SystemPropertyKeys.forceSingle.name()),
                node.getProperty(SystemPropertyKeys.description.name()));
    }

    
    private String getSignature(Node node, String name) {
        return CypherProceduresHandler.deserializeSignatures((String) node.getProperty(name))
                .stream().map(FieldSignature::toString)
                .collect(Collectors.joining(", "));
    }

    private void addToExportList(Map<String, List<String>> exportApoc, SystemDbConfig systemDbConfig, String feature, String statement, Node node) {
        addToExportList(exportApoc, systemDbConfig, feature, statement, node, "");
    }

    private void addToExportList(Map<String, List<String>> exportApoc, SystemDbConfig systemDbConfig, String feature, String statement, Node node, String suffix) {
        final List<String> features = systemDbConfig.getFeatures();
        if (!features.contains(feature)) {
            return;
        }
        // we create a file featureName.dbName because there could be features coming from different databases
        String dbName = (String) node.getProperty(SystemPropertyKeys.database.name(), null);
        dbName = StringUtils.isEmpty(dbName) ? StringUtils.EMPTY : "." + dbName;
        
        exportApoc.compute(feature + suffix + dbName, (k, v) -> {
            if (v == null) {
                return new ArrayList<>(List.of(statement));
            }
            v.add(statement);
            return v;
        });
    }


    @Procedure
    public Stream<NodesAndRelationshipsResult> graph() {
        Util.checkAdmin(securityContext, callContext,"apoc.systemdb.graph");
        return withSystemDbTransaction(tx -> {
            Map<Long, Node> virtualNodes = new HashMap<>();
            for (Node node: tx.getAllNodes())  {
                virtualNodes.put(-node.getId(), new VirtualNode(-node.getId(), Iterables.asArray(Label.class, node.getLabels()), node.getAllProperties()));
            }

            List<Relationship> relationships = tx.getAllRelationships().stream().map(rel -> new VirtualRelationship(
                    -rel.getId(),
                    virtualNodes.get(-rel.getStartNodeId()),
                    virtualNodes.get(-rel.getEndNodeId()),
                    rel.getType(),
                    rel.getAllProperties())).collect(Collectors.toList()
            );
            return Stream.of(new NodesAndRelationshipsResult(Iterables.asList(virtualNodes.values()), relationships) );
        });
    }

    @Procedure
    public Stream<RowResult> execute(@Name("DDL commands, either a string or a list of strings") Object ddlStringOrList, @Name(value="params", defaultValue = "{}") Map<String ,Object> params) {
        Util.checkAdmin(securityContext, callContext, "apoc.systemdb.execute");

        List<String> commands;
        if (ddlStringOrList instanceof String) {
            commands = Collections.singletonList((String)ddlStringOrList);
        } else if (ddlStringOrList instanceof List) {
            commands = (List<String>) ddlStringOrList;
        } else {
            throw new IllegalArgumentException("don't know how to handle " + ddlStringOrList + ". Supply either a string or a list of strings");
        }

        Transaction tx = apocConfig.getSystemDb().beginTx();  // we can't use try-with-resources otherwise tx gets closed too early
        return commands.stream().flatMap(command -> tx.execute(command, params).stream().map(RowResult::new)).onClose(() -> {
            boolean isOpen = ((TransactionImpl) tx).isOpen(); // no other way to check if a tx is still open
            if (isOpen) {
                tx.commit();
            }
            tx.close();
        });
    }

    private <T> T withSystemDbTransaction(Function<Transaction, T> function) {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            T result = function.apply(tx);
            tx.commit();
            return result;
        }
    }
}
