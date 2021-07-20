package apoc.cypher;

import apoc.ApocConfig;
import apoc.util.Util;
import org.apache.commons.configuration2.Configuration;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static apoc.ApocConfig.APOC_CONFIG_STRICT_INITIALIZER;
import static org.apache.commons.collections.IteratorUtils.chainedIterator;

public class CypherInitializer implements AvailabilityListener {

    private final GraphDatabaseAPI db;
    private final DatabaseManagementService dbms;
    private final Log userLog;
    private final GlobalProcedures procs;
    private final DependencyResolver dependencyResolver;

    /**
     * indicates the status of the initializer, to be used for tests to ensure initializer operations are already done
     */
    private boolean finished = false;

    public CypherInitializer(GraphDatabaseAPI db, DatabaseManagementService dbms, Log userLog) {
        this.db = db;
        this.dbms = dbms;
        this.userLog = userLog;
        this.dependencyResolver = db.getDependencyResolver();
        this.procs = dependencyResolver.resolveDependency(GlobalProcedures.class);
    }

    public boolean isFinished() {
        return finished;
    }

    public GraphDatabaseAPI getDb() {
        return db;
    }

    @Override
    public void available() {

        // run initializers in a new thread
        // we need to wait until apoc procs are registered
        // unfortunately an AvailabilityListener is triggered before that
        Util.newDaemonThread(() -> {
            try {
                final boolean isSystemDatabase = db.databaseName().equals(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
                if (!isSystemDatabase) {
                    awaitApocProceduresRegistered();
                }
                Configuration config = dependencyResolver.resolveDependency(ApocConfig.class).getConfig();

                for (Map.Entry<String, String> entry : collectInitializers(isSystemDatabase, config).entrySet()) {
                    final String key = entry.getKey();
                    final String query = entry.getValue();
                    try {
                        // we need to apply a retry strategy here since in systemdb we potentially conflict with
                        // creating constraints which could cause our query to fail with a transient error.
                        Util.retryInTx(userLog, db, tx -> Iterators.count(tx.execute(query)), 0, 5, retries -> { });
                        userLog.info("successfully initialized: " + query);
                    } catch (Exception e) {
                        userLog.error("error upon initialization, running: " + query, e);
                        
                        if (key.startsWith(APOC_CONFIG_STRICT_INITIALIZER)) {
                            dbms.shutdown();
                        }
                    }
                }
            } finally {
                finished = true;
            }
        }).start();
    }

    private Map<String, String> collectInitializers(boolean isSystemDatabase, Configuration config) {
        Map<String, String> initializers = new TreeMap<>();


        final String suffix = "." + db.databaseName();
        final Iterator<String> iterator = chainedIterator(config.getKeys(ApocConfig.APOC_CONFIG_INITIALIZER + suffix),
                config.getKeys(APOC_CONFIG_STRICT_INITIALIZER + suffix));
        
        iterator.forEachRemaining(key -> putIfNotBlank(initializers, key, config.getString(key)));
        
        // add legacy style initializers
        if (!isSystemDatabase) {
            config.getKeys(ApocConfig.APOC_CONFIG_INITIALIZER_CYPHER)
                    .forEachRemaining(key -> initializers.put(key, config.getString(key)));
        }

        return initializers;
    }

    private void putIfNotBlank(Map<String,String> map, String key, String value) {
        if ((value!=null) && (!value.isBlank())) {
            map.put(key, value);
        }
    }

    private void awaitApocProceduresRegistered() {
        while (!areApocProceduresRegistered()) {
            Util.sleep(100);
        }
    }

    private boolean areApocProceduresRegistered() {
        try {
            return procs.getAllProcedures().stream().anyMatch(signature -> signature.name().toString().startsWith("apoc"));
        } catch (ConcurrentModificationException e) {
            // if a CME happens (possible during procedure scanning)
            // we return false and the caller will try again
            return false;
        }
    }

    @Override
    public void unavailable() {
        // intentionally empty
    }
}
