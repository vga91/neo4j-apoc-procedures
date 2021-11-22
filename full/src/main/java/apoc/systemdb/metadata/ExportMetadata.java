package apoc.systemdb.metadata;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.Map;
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
        
        public Stream<Map.Entry<String, String>> export(Node node) {
            return exportMetadata.export(node);
        }
        
        public static Type from(String type) {
            return Stream.of(Type.values())
                    .filter(t -> t.name().equalsIgnoreCase(type))
                    .findFirst()
                    .get();
        }
        
        public static Optional<Type> from(Label label) {
            final String name = label.name();
            if (name.equals(SystemLabels.Procedure.name())) {
                return Optional.of(CypherProcedure);
            } else if(name.equals(SystemLabels.Function.name())) {
                return Optional.of(CypherFunction);
            } else if(name.equals(SystemLabels.ApocTrigger.name())) {
                return Optional.of(Trigger);
            } else if(name.equals(SystemLabels.ApocUuid.name())) {
                return Optional.of(Uuid);
            } else if(name.equals(SystemLabels.DataVirtualizationCatalog.name())) {
                return Optional.of(DataVirtualizationCatalog);
            }
            return Optional.empty();
        }
    }

    Stream<Map.Entry<String, String>> export(Node node);
    
    default String toNeo4jStringMap(Map<String, Object> map) {
        try {
            return new ObjectMapper()
                    .disable(JsonGenerator.Feature.QUOTE_FIELD_NAMES)
                    .writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
    
    default String getFileName(Node node, String prefix) {
        // we create a file featureName.dbName because there could be features coming from different databases
        String dbName = (String) node.getProperty(SystemPropertyKeys.database.name(), null);
        dbName = StringUtils.isEmpty(dbName) ? StringUtils.EMPTY : "." + dbName;
        return prefix + dbName;
    }
}
