package apoc.uuid;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.Pools;
import apoc.util.SystemDbUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.procedure.*;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.uuid.Uuid.getExistingNodesResult;
import static apoc.uuid.UuidHandler.APOC_UUID_REFRESH;

@Extended
public class UuidNewProcedures {
    public static final String UUID_NOT_SET = APOC_UUID_REFRESH + " is not set. Please please set it in your apoc.conf";

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public ApocConfig apocConfig;

    private void checkInSystemLeader() {
        checkEnabled();
        checkConfigSet();

        SystemDbUtil.checkInSystemLeader(db);
    }

    private void checkInSystem() {
        checkEnabled();
        SystemDbUtil.checkInSystem(db);
    }

    private void checkEnabled() {
        UuidHandlerNewProcedures.checkEnabled(apocConfig, db.databaseName());
    }

    private void checkTargetDatabase(String databaseName) {
        SystemDbUtil.checkTargetDatabase(databaseName, "Automatic UUIDs");
    }


    // todo - credo debba andare anche al remove, metti che riavvio
    // allora fare metodo comune
    private void checkConfigSet() {
        //
        if (apocConfig.getConfig().getInteger(APOC_UUID_REFRESH, null) == null) {
            throw new RuntimeException(UUID_NOT_SET);
        }
    }

    @Context
    public UuidHandler uuidHandler;

    @Context
    public Pools pools;

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("TODO")
    public Stream<UuidInstallInfo> create(@Name("databaseName") String databaseName,
                                               @Name("label") String label,
                                               @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        System.out.println("databaseName = " + databaseName);
        // todo-- UuidHandlerWrite

//        checkConfigSet();

        checkInSystemLeader();
        checkTargetDatabase(databaseName);


        UuidConfig uuidConfig = new UuidConfig(config);
        // todo - delete
        System.out.println("uuidHandler = " + uuidHandler);
//        if (uuidHandler != null) {
//            uuidHandler.add(tx, label, uuidConfig);
//        }

        // TODO - the ApocConfig.apocConfig().getDatabase(databaseName) has to be deleted in 5.x,
        //  because in a cluster, not all DBMS host all the databases on them,
        //  so we have to assume that the leader of the system database doesn't have access to this user database
        GraphDatabaseService db = apocConfig.getDatabase(databaseName);

        //  TODO - in 5.x maybe we could put it in UuidHandler.java and execute it in the refresh() method before all
        try (Transaction tx = db.beginTx()) {
            UuidHandlerNewProcedures.checkConstraintUuid(tx, label, uuidConfig.getUuidProperty());
        }

        //  TODO - in 5.x maybe we could put it in UuidHandler.java and execute it in the refresh() method before all
        Map<String, Object> addToExistingNodesResult = getExistingNodesResult(db, pools, label, uuidConfig);
//        Map<String, Object> addToExistingNodesResult = Collections.emptyMap();
//        if (uuidConfig.isAddToExistingNodes()) {
//            final String uuidFunctionName = getUuidFunctionName();
//            addToExistingNodesResult = Util.inTx(db, pools, txInThread ->
//                    txInThread.execute("CALL apoc.periodic.iterate(" +
//                                    "\"MATCH (n:" + Util.sanitizeAndQuote(label) + ") RETURN n\",\n" +
//                                    "\"SET n." + Util.sanitizeAndQuote(uuidConfig.getUuidProperty()) + " = " + uuidFunctionName + "()\", {batchSize:10000, parallel:true})")
//                            .next()
//            );
//        }


        /*UuidInstallInfo uuidInfo = */UuidHandlerNewProcedures.create(databaseName, label, uuidConfig);

        UuidInstallInfo uuidInstallInfo = UuidInstallInfo.from(label, addToExistingNodesResult, uuidConfig);
        return Stream.of(uuidInstallInfo);
    }





    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.drop(label) yield label, installed, properties | remove previously added uuid handler and returns uuid information. All the existing uuid properties are left as-is")
    public Stream<UuidInfo> drop(@Name("databaseName") String databaseName, @Name("label") String label) {
        checkInSystemLeader();

        final UuidInfo uuidInfo = UuidHandlerNewProcedures.drop(databaseName, label);
        return Stream.ofNullable(uuidInfo);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.dropAll() yield label, installed, properties | it removes all previously added uuid handlers and returns uuids information. All the existing uuid properties are left as-is")
    public Stream<UuidInfo> dropAll(@Name("databaseName") String databaseName) {
        checkInSystemLeader();

        return UuidHandlerNewProcedures.dropAll(databaseName)
                .stream()
                .sorted(Comparator.comparing(i -> i.label));
    }


    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.READ)
    @Description("CALL apoc.uuid.show(databaseName) | it lists all eventually installed TODO for a database")
    public Stream<UuidInfo> show(@Name("databaseName") String databaseName) {
        checkInSystem();

        return UuidHandlerNewProcedures.getUuidNodesList(databaseName, tx);
    }

}
