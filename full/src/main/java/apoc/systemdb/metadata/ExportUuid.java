package apoc.systemdb.metadata;

import apoc.SystemPropertyKeys;
import org.neo4j.graphdb.Node;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.systemdb.SystemDbConfig.UUIDS;

public class ExportUuid implements ExportMetadata {

    @Override
    public Stream export(Node node) {
        Map<String, Object> map = new HashMap<>();
        final String labelName = (String) node.getProperty(SystemPropertyKeys.label.name());
        final String property = (String) node.getProperty(SystemPropertyKeys.propertyName.name());
        map.put("uuidProperty", property);
        map.put("addToSetLabels", node.getProperty(SystemPropertyKeys.addToSetLabel.name(), null));
        final String uuidConfig = toNeo4jStringMap(map);
        // add constraint - TODO: might be worth add config to export or not this file
        String schemaStatement = String.format("CREATE CONSTRAINT IF NOT EXISTS ON (n:%s) ASSERT n.%s IS UNIQUE;\n", labelName, property);
        final String statement = String.format("CALL apoc.uuid.install('%s', %s);", labelName, uuidConfig);
        
        return Stream.of(
                new AbstractMap.SimpleEntry<>(getFileName(node, UUIDS + ".schema"), schemaStatement),
                new AbstractMap.SimpleEntry<>(getFileName(node, UUIDS), statement)
        );
                
    }
}
