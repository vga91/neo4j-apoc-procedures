package apoc.uuid;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.SystemDbUtil;
import apoc.util.Util;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.ApocConfig.*;
import static apoc.SystemPropertyKeys.*;
import static apoc.SystemLabels.*;
import static apoc.util.SystemDbUtil.getSystemNodes;
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.uuid.UuidHandler.NOT_ENABLED_ERROR;

public class UUIDHandlerNewProcedures {
    public static boolean isEnabled(String databaseName) {
        String apocUUIDEnabledDb = String.format(ApocConfig.APOC_UUID_ENABLED_DB, databaseName);
        return apocConfig().getConfig().getBoolean(apocUUIDEnabledDb, apocConfig().getBoolean(APOC_UUID_ENABLED));
    }

    public static void checkEnabled(String databaseName) {
        if (!isEnabled(databaseName)) {
            String error = String.format(NOT_ENABLED_ERROR, databaseName);
            throw new RuntimeException(error);
        }
    }

    public static UuidInfo create(String databaseName, String label,  UuidConfig config) {
        final UuidInfo[] result = new UuidInfo[1];

        withSystemDb(sysTx -> {
            Node node = Util.mergeNode(sysTx, SystemLabels.ApocUuid, null,
                    Pair.of(database.name(), databaseName),
                    Pair.of(SystemPropertyKeys.label.name(), label)
            );

            node.setProperty(propertyName.name(), config.getUuidProperty());
            node.setProperty(addToSetLabel.name(), config.isAddToSetLabels());
            node.setProperty(addToExistingNodes.name(), config.isAddToExistingNodes());

            // we'll the return current uuid info
            result[0] = UuidInfo.fromNode(node, true);

            setLastUpdate(databaseName, sysTx);
        });

        return result[0];
    }

    public static UuidInfo drop(String databaseName, String labelName) {
        final UuidInfo[] previous = new UuidInfo[1];

        withSystemDb(tx -> {
            getUuidNodes(databaseName, tx, Map.of(SystemPropertyKeys.label.name(), labelName))
                    .forEachRemaining(node -> {
                        previous[0] = UuidInfo.fromNode(node);
                        node.delete();
                    });

            setLastUpdate(databaseName, tx);
        });

        return previous[0];
    }

    public static List<UuidInfo> dropAll(String databaseName) {
        final List<UuidInfo> previous = new ArrayList<>();

        withSystemDb(tx -> {
            getUuidNodes(databaseName, tx)
                    .forEachRemaining(node -> {
                        // we'll return previous uuid info
                        previous.add( UuidInfo.fromNode(node) );
                        node.delete();
                    });

            setLastUpdate(databaseName, tx);
        });

        return previous;
    }

    public static Stream<UuidInfo> getUuidNodesList(String databaseName, Transaction tx) {
        return getUuidNodes(databaseName, tx)
                .stream()
                .map(UuidInfo::fromNode);
    }

    public static ResourceIterator<Node> getUuidNodes(String databaseName, Transaction tx) {
        return getUuidNodes(databaseName, tx, null);
    }

    public static ResourceIterator<Node> getUuidNodes(String databaseName, Transaction tx, Map<String, Object> props) {
        return getSystemNodes(databaseName, tx, SystemLabels.ApocUuid, props);
    }

    public static void checkConstraintUuid(Transaction tx, String label, String propertyName) {
        Schema schema = tx.schema();
        Stream<ConstraintDefinition> constraintDefinitionStream = StreamSupport.stream(schema.getConstraints(Label.label(label)).spliterator(), false);
        boolean exists = constraintDefinitionStream.anyMatch(constraint -> {
            Stream<String> streamPropertyKeys = StreamSupport.stream(constraint.getPropertyKeys().spliterator(), false);
            return streamPropertyKeys.anyMatch(property -> property.equals(propertyName));
        });
        if (!exists) {
            String error = String.format("`CREATE CONSTRAINT ON (%s:%s) ASSERT %s.%s IS UNIQUE`",
                    label.toLowerCase(), label, label.toLowerCase(), propertyName);
            throw new RuntimeException("No constraint found for label: " + label + ", please add the constraint with the following : " + error);
        }
    }


    private static void setLastUpdate(String databaseName, Transaction tx) {
        SystemDbUtil.setLastUpdate(databaseName, tx, ApocUuidMeta);
    }
}
