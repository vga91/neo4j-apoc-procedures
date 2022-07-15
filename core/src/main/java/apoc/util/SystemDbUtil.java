package apoc.util;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import java.util.Map;
import java.util.function.Function;

import static apoc.ApocConfig.apocConfig;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class SystemDbUtil {
    public static final String KEY_CURRENT_DB = "apoc.storecurrentdb"; // todo - e se lo chiamassi storethisdb????
    
//    public static final 
    
    // todo - SystemLabel.ApocCypherProcedures --> appartiene a tutti, posso prendere questo come feature
    // todo - creare un enum o qualcosa di simiie con le key
//    private static final Map<String, SystemLabels> featureMap = Map.of("custom", )

    public static boolean isCurrentDb(GraphDatabaseService db, String featureName) {
        // todo - forse non serve.. verificare
        if (db.databaseName().equals(SYSTEM_DATABASE_NAME)) {
            return false;
        }
        final String currentDbNameKey = String.format("%s.%s", KEY_CURRENT_DB, db.databaseName());
        final String currentDbAndFeatureKey = String.format("%s.%s.%s", KEY_CURRENT_DB, db.databaseName(), featureName);

        return apocConfig().getBoolean(currentDbAndFeatureKey,
                apocConfig().getBoolean(currentDbNameKey,
                        apocConfig().getBoolean(KEY_CURRENT_DB, false))
        );
    }


    public static <T> T migrate(GraphDatabaseService db, String featureName) {
        // todo - common???
        final GraphDatabaseService currentDb;
        final GraphDatabaseService otherDb;
        if (isCurrentDb(db, featureName)) {
            currentDb = db;
            otherDb = apocConfig().getSystemDb();
        } else {
            currentDb = apocConfig().getSystemDb();
            otherDb = db;
        }

//        final Transaction transaction = db.beginTx();
//        transaction.findNodes(SystemLabels.ApocTrigger,
//                SystemPropertyKeys.database.name(), db.databaseName()).forEachRemaining(node -> {
//                    node.
//        });
// MATCH (f:Person {name:'Foo'}), (b:Person {surname:'Bar'})
//CALL apoc.refactor.mergeNodes([f,b])
//YIELD node RETURN node

        // TODO TODOISSIMO --> setLastUpdate(Transaction tx) farlo anche al migrate
                
        return null;
    }

    public static <T> T todoOtherDb(GraphDatabaseService db, String featureName, Function<Transaction, T> action) {
        final GraphDatabaseService currentDb = isCurrentDb(db, featureName)
                ? apocConfig().getSystemDb() : db;

        return getT(action, currentDb);
    }

    private static <T> T getT(Function<Transaction, T> action, GraphDatabaseService currentDb) {
        try (Transaction tx = currentDb.beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    public static <T> T todoThisDb(GraphDatabaseService db, String featureName, Function<Transaction, T> action) {
        final GraphDatabaseService currentDb = isCurrentDb(db, featureName)
                ? db : apocConfig().getSystemDb();

        return getT(action, currentDb);
//        try (Transaction tx = currentDb.beginTx()) {
//            T result = action.apply(tx);
//            tx.commit();
//            return result;
//        }
    }

//    private static  <T> T withSystemDb(Function<Transaction, T> action) {
//        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
//            T result = action.apply(tx);
//            tx.commit();
//            return result;
//        }
//    }
}
