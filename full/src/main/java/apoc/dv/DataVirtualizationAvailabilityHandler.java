//package apoc.dv;
//
//import org.neo4j.dbms.api.DatabaseManagementService;
//import org.neo4j.graphdb.event.DatabaseEventContext;
//import org.neo4j.graphdb.event.DatabaseEventListener;
//import org.neo4j.kernel.internal.GraphDatabaseAPI;
//import org.neo4j.kernel.lifecycle.LifecycleAdapter;
//
//public class DataVirtualizationAvailabilityHandler/* extends LifecycleAdapter */implements DatabaseEventListener {
//
////    private final DatabaseManagementService databaseManagementService;
//
//    public DataVirtualizationAvailabilityHandler(/*DatabaseManagementService databaseManagementService*/) {
////        this.databaseManagementService = databaseManagementService;
//    }
//
//    @Override
//    public void databaseStart(DatabaseEventContext eventContext) {
//        // todo - common method
//        System.out.println("eventContext.AJEJE() = " + eventContext.getDatabaseName());
//    }
//
//    @Override
//    public void databaseShutdown(DatabaseEventContext eventContext) {
//
//    }
//
//    @Override
//    public void databasePanic(DatabaseEventContext eventContext) { }
//
////    @Override
////    public void start() {
////        databaseManagementService.registerDatabaseEventListener(this);
////    }
////
////    @Override
////    public void stop() {
//////        removeAll();
////    }
//}
