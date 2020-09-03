package apoc.ttl;

import apoc.ApocConfig;
import apoc.util.Util;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author mh
 * @since 15.02.17
 */
public class TTLLifeCycle extends LifecycleAdapter {

    public static final int INITIAL_DELAY = 30;
    public static final int DEFAULT_SCHEDULE = 60;
    private static final Group TTL_GROUP = Group.INDEX_UPDATING;
    private final JobScheduler scheduler;
    private final GraphDatabaseAPI db;
    private final ApocConfig apocConfig;
    private JobHandle ttlIndexJobHandle;
    private JobHandle ttlJobHandle;
    private Log log;

    public TTLLifeCycle(JobScheduler scheduler, GraphDatabaseAPI db, ApocConfig apocConfig, Log log) {
        this.scheduler = scheduler;
        this.db = db;
        this.apocConfig = apocConfig;
        this.log = log;
    }

    @Override
    public void start() {
        boolean enabled = apocConfig.getBoolean(ApocConfig.APOC_TTL_ENABLED);
        if (enabled) {
            long ttlSchedule = apocConfig.getInt(ApocConfig.APOC_TTL_SCHEDULE, DEFAULT_SCHEDULE);
            ttlIndexJobHandle = scheduler.schedule(TTL_GROUP, this::createTTLIndex, (int)(ttlSchedule*0.8), TimeUnit.SECONDS);
            long limit = apocConfig.getInt(ApocConfig.APOC_TTL_LIMIT, 1000);
            long batchSize = apocConfig.getInt(ApocConfig.APOC_TTL_BATCH_SIZE, 10000);
            ttlJobHandle = scheduler.scheduleRecurring(TTL_GROUP, () -> expireNodes(limit, batchSize), ttlSchedule, ttlSchedule, TimeUnit.SECONDS);
        }
    }

    public void expireNodes(long limit, long batchSize) {
        try {
            if (!Util.isWriteableInstance(db)) return;
            String withLimit = (limit > 0) ? "LIMIT $limit" : "";
            String matchTTL = "MATCH (t:TTL) WHERE t.ttl < timestamp() ";
            String queryRels = matchTTL + "WITH t " + withLimit + " MATCH (t)-[r]-() RETURN r";
            String queryNodes = matchTTL +  "RETURN t " + withLimit;
            Map<String,Object> params = Util.map("limit", limit, "batchSize", batchSize, "queryRels", queryRels, "queryNodes", queryNodes);
            long relationshipsDeleted = db.executeTransactionally(
                    "CALL apoc.periodic.iterate($queryRels, 'DELETE r', {batchSize: $batchSize, params:{limit: $limit}})",
                    params,
                    result -> Iterators.single(result.columnAs("total"))
            );

            long nodesDeleted = db.executeTransactionally(
                    "CALL apoc.periodic.iterate($queryNodes, 'DELETE t', {batchSize: $batchSize, params:{limit: $limit}})",
                    params,
                    result -> Iterators.single(result.columnAs("total"))
            );

            if (nodesDeleted > 0) {
                log.info("TTL: Expired %d nodes %d relationships", nodesDeleted, relationshipsDeleted);
            }
        } catch (Exception e) {
            log.error("TTL: Error deleting expired nodes", e);
        }
    }

    public void createTTLIndex() {
        try {
            db.executeTransactionally("CREATE INDEX ON :TTL(ttl)");
        } catch (Exception e) {
            log.error("TTL: Error creating index", e);
        }
    }

    @Override
    public void stop() {
        if (ttlIndexJobHandle != null) ttlIndexJobHandle.cancel();
        if (ttlJobHandle != null) ttlJobHandle.cancel();
    }
}
