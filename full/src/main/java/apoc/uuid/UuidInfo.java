package apoc.uuid;

import apoc.SystemPropertyKeys;
import org.neo4j.graphdb.Node;

import java.util.Collections;
import java.util.Map;

import static apoc.uuid.UuidConfig.ADD_TO_SET_LABELS_KEY;
import static apoc.uuid.UuidConfig.UUID_PROPERTY_KEY;

public class UuidInfo {
    public final String label;
    public boolean installed;
    public Map<String, Object> properties;

    UuidInfo(String label, boolean installed, Map<String, Object> properties) {
        this.label = label;
        this.installed = installed;
        this.properties = properties;
    }

    UuidInfo(String label, boolean installed) {
        this(label, installed, Collections.emptyMap());
    }

    public static UuidInfo fromNode(Node node, boolean installed) {
        String label = (String) node.getProperty(SystemPropertyKeys.label.name());
        boolean addToSetLabel = (boolean) node.getProperty(SystemPropertyKeys.addToSetLabel.name());
        String propertyName = (String) node.getProperty(SystemPropertyKeys.propertyName.name());
        Map<String, Object> properties = Map.of(UUID_PROPERTY_KEY, propertyName,
                ADD_TO_SET_LABELS_KEY, addToSetLabel);
        return new UuidInfo(label, installed, properties);
    }

    public static UuidInfo fromNode(Node node) {
        return fromNode(node, false);
    }
}