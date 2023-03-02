package apoc.util;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.trigger.TriggerHandlerNewProcedures;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static apoc.ApocConfig.apocConfig;
import static apoc.SystemPropertyKeys.database;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class SystemDbUtil {
    public static final String SYS_NON_LEADER_ERROR = "It's not possible to write into a cluster member with a non-LEADER system database.\n";
    public static final String DB_NOT_FOUND_ERROR = "The user database with name '%s' does not exist";


    public static final String NON_SYS_DB_ERROR = "The procedure should be executed against a system database.";
    public static final String PROCEDURE_NOT_ROUTED_ERROR = "No write operations are allowed directly on this database. " +
            "Writes must pass through the leader. The role of this server is: FOLLOWER";

    public static final String BAD_TARGET_ERROR = " can only be installed on user databases.";


    public static void preprocessDeprecatedProcedures(GraphDatabaseService db, String msgDeprecation) {
        if (!Util.isWriteableInstance(db, GraphDatabaseSettings.SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(SYS_NON_LEADER_ERROR + msgDeprecation);
        }
    }

    public static void checkInSystem(GraphDatabaseService db) {
//        TriggerHandlerNewProcedures.checkEnabled();

        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(NON_SYS_DB_ERROR);
        }
    }

    public static void checkInSystemLeader(GraphDatabaseService db) {
        // todo - change it... Interface?
//        TriggerHandlerNewProcedures.checkEnabled();
        // routing check
        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME) || !Util.isWriteableInstance(db, SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(PROCEDURE_NOT_ROUTED_ERROR);
        }
    }

    public static void checkTargetDatabase(/*Transaction tx, */String databaseName, String type) {
//        final Set<String> databases = tx.execute("SHOW DATABASES", Collections.emptyMap())
//                .<String>columnAs("name")
//                .stream()
//                .collect(Collectors.toSet());
//        if (!databases.contains(databaseName)) {
//            throw new RuntimeException( String.format(DB_NOT_FOUND_ERROR, databaseName) );
//        }

        if (databaseName.equals(SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(type + BAD_TARGET_ERROR);
        }
    }

    public static <T> T withSystemDb(Function<Transaction, T> action) {
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    public static void withSystemDb(Consumer<Transaction> consumer) {
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            consumer.accept(tx);
            tx.commit();
        }
    }

    // todo - maybe not used
    public static ResourceIterator<Node> getSystemNodes(String databaseName, Transaction tx, SystemLabels sysLabel) {
        return getSystemNodes(databaseName, tx, sysLabel, null);
    }

    public static ResourceIterator<Node> getSystemNodes(String databaseName, Transaction tx, /*String prop,*/
                                                        SystemLabels sysLabel,
                                                        Map<String, Object> props) {
//        final SystemLabels sysLabel = SystemLabels.ApocUuid;
        final String dbNameKey = database.name();

        // search all system nodes
        if (props == null) {
            return tx.findNodes(sysLabel, dbNameKey, databaseName);
        }

        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(dbNameKey, databaseName);
        propsMap.putAll(props);

        return tx.findNodes(sysLabel, propsMap);
                // todo - this key is prop instead of name like in trigger
//                Map.of(dbNameKey, databaseName,
//                        SystemPropertyKeys.label.name(), prop)
//        );
    }

    public static void setLastUpdate(String databaseName, Transaction tx, SystemLabels label) {
        Node node = tx.findNode(label, database.name(), databaseName);
        if (node == null) {
            node = tx.createNode(label);
            node.setProperty(database.name(), databaseName);
        }
        final long value = System.currentTimeMillis();
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), value);
    }
}
