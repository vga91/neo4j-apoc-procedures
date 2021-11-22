package apoc.systemdb.metadata;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.Optional;
import java.util.stream.Stream;

public interface ExportMetadata {
    
    enum Type {
        CypherProcedure(new ExportProcedure()),
        CypherFunction(new ExportFunction()),
        Uuid(new ExportUuid()),
        Trigger(new ExportTrigger()),
        DataVirtualizationCatalog(new ExportDataVirtualization());

        private final ExportMetadata exportMetadata;

        Type(ExportMetadata exportMetadata) {
            this.exportMetadata = exportMetadata;
        }
        
        public Stream<Pair<String, String>> export(Node node) {
            return exportMetadata.export(node);
        }
        
        public static Optional<Type> from(Label label) {
            final String name = label.name();
            if (name.equalsIgnoreCase(SystemLabels.Procedure.name())) {
                return Optional.of(CypherProcedure);
            } else if(name.equalsIgnoreCase(SystemLabels.Function.name())) {
                return Optional.of(CypherFunction);
            } else if(name.equalsIgnoreCase(SystemLabels.ApocTrigger.name())) {
                return Optional.of(Trigger);
            } else if(name.equalsIgnoreCase(SystemLabels.ApocUuid.name())) {
                return Optional.of(Uuid);
            } else if(name.equalsIgnoreCase(SystemLabels.DataVirtualizationCatalog.name())) {
                return Optional.of(DataVirtualizationCatalog);
            }
            return Optional.empty();
        }
    }

    Stream<Pair<String, String>> export(Node node);
    
    default String getFileName(Node node, String prefix) {
        // we create a file featureName.dbName because there could be features coming from different databases
        String dbName = (String) node.getProperty(SystemPropertyKeys.database.name(), null);
        dbName = StringUtils.isEmpty(dbName) ? StringUtils.EMPTY : "." + dbName;
        return prefix + dbName;
    }
}
