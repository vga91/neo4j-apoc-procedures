package apoc.uuid;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.trigger.TriggerInfo;
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
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import apoc.SystemPropertyKeys.*;

import static apoc.ApocConfig.*;
import static apoc.SystemPropertyKeys.*;
import static apoc.SystemLabels.*;
import static apoc.uuid.UuidInfo.fromNode;
import static apoc.uuid.UuidHandler.NOT_ENABLED_ERROR;

public class UuidHandlerNewProcedures {
    public static boolean isEnabled(ApocConfig apocConfig, String databaseName) {
        String apocUUIDEnabledDb = String.format(ApocConfig.APOC_UUID_ENABLED_DB, databaseName);
        return apocConfig.getConfig().getBoolean(apocUUIDEnabledDb, apocConfig.getBoolean(APOC_UUID_ENABLED));
    }

    public static void checkEnabled(ApocConfig apocConfig, String databaseName) {
        if (!isEnabled(apocConfig, databaseName)) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    // todo -move UuidInstallInfo in new separate Class
    public static void create(String databaseName, String label,  UuidConfig config) {

        withSystemDb(sysTx -> {
            Node node = Util.mergeNode(sysTx, SystemLabels.ApocUuid, null,
                    Pair.of(database.name(), databaseName),
                    Pair.of(SystemPropertyKeys.label.name(), label)
            );

            node.setProperty(propertyName.name(), config.getUuidProperty());
            node.setProperty(addToSetLabel.name(), config.isAddToSetLabels());


            setLastUpdate(databaseName, sysTx);

            // we'll the return current trigger info
//            result[0] = UuidInfo.fromNode(node, true);

//            setLastUpdate(databaseName, tx);
        });
    }

//    public static List<UuidInstallInfo> dropAll(String databaseName) {
//        final List<UuidInstallInfo> previous = new ArrayList<>();
//
//        withSystemDb(tx -> {
//            getUuidNodes(databaseName, tx)
//                    .forEachRemaining(node -> {
//                        // we'll return previous auto uuid info
//                        previous.add( UuidInstallInfo.fromNode(node, false) );
//                        node.delete();
//                    });
////            setLastUpdate(databaseName, tx);
//        });
//
//        return previous;
//    }

    // todo - common?
    public static UuidInfo drop(String databaseName, String triggerName) {
        final UuidInfo[] previous = new UuidInfo[1];

        withSystemDb(tx -> {
            getUuidNodes(databaseName, tx, triggerName)
                    .forEachRemaining(node -> {
                        previous[0] = UuidInfo.fromNode(node, false);
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
                        previous.add( UuidInfo.fromNode(node, false) );
                        node.delete();
                    });

            setLastUpdate(databaseName, tx);
        });

        return previous;
    }

    // todo - common method in SystemDbUtils
    public static ResourceIterator<Node> getUuidNodes(String databaseName, Transaction tx) {
        return getUuidNodes(databaseName, tx, null);
    }

    // todo - common method in SystemDbUtils
    public static ResourceIterator<Node> getUuidNodes(String databaseName, Transaction tx, String label) {
        final SystemLabels sysLabel = SystemLabels.ApocUuid;
        final String dbNameKey = database.name();
        if (label == null) {
            return tx.findNodes(sysLabel, dbNameKey, databaseName);
        }
        return tx.findNodes(sysLabel, dbNameKey, databaseName,
                // todo - this key is label instead of name like in trigger
                SystemPropertyKeys.label.name(),
                label);
    }

    // todo - common method in SystemDbUtils
    public static Stream<UuidInfo> getUuidNodesList(String databaseName, Transaction tx) {
        return getUuidNodes(databaseName, tx)
                .stream()
                .map(UuidInfo::fromNode);
    }

    // TODO - common
    public static void withSystemDb(Consumer<Transaction> consumer) {
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            consumer.accept(tx);
            tx.commit();
        }
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


    // todo - common
    private static void setLastUpdate(String databaseName, Transaction tx) {
        Node node = tx.findNode(SystemLabels.ApocUuidMeta, database.name(), databaseName);
        if (node == null) {
            node = tx.createNode(SystemLabels.ApocUuidMeta);
            node.setProperty(database.name(), databaseName);
        }
        final long value = System.currentTimeMillis();
        System.out.println("value = " + value);
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), value);
    }

//    private static void setLastUpdate(String databaseName, Transaction tx) {
//        Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), databaseName);
//        if (node == null) {
//            node = tx.createNode(SystemLabels.ApocTriggerMeta);
//            node.setProperty(SystemPropertyKeys.database.name(), databaseName);
//        }
//        final long value = System.currentTimeMillis();
//        node.setProperty(SystemPropertyKeys.lastUpdated.name(), value);
//    }
}
