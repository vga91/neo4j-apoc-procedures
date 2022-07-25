package apoc;

import apoc.custom.CypherProcedures;
import apoc.custom.CypherProceduresHandler;
import apoc.dv.DataVirtualizationCatalog;
import apoc.dv.DataVirtualizationCatalogHandler;
import apoc.load.LoadDirectory;
import apoc.load.LoadDirectoryHandler;
import apoc.ttl.TTLLifeCycle;
import apoc.uuid.Uuid;
import apoc.uuid.UuidHandler;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ServiceProvider
public class ExtendedApocGlobalComponents implements ApocGlobalComponents {

    private final Map<GraphDatabaseService,CypherProceduresHandler> cypherProcedureHandlers = new ConcurrentHashMap<>();
    private List<DatabaseEventListener> lists = new ArrayList<>();

    @Override
    public Map<String, Lifecycle> getServices(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {

//        dependencies.databaseManagementService().registerDatabaseEventListener(new DataVirtualizationAvailabilityHandler());

        CypherProceduresHandler cypherProcedureHandler = new CypherProceduresHandler(
                db,
                dependencies.databaseManagementService(), // todo - forse si può rimuovere
                dependencies.scheduler(),
                dependencies.apocConfig(),
                dependencies.log().getUserLog(CypherProcedures.class),
                dependencies.globalProceduresRegistry()
        );
        cypherProcedureHandlers.put(db, cypherProcedureHandler);

        final UuidHandler uuidHandler = new UuidHandler(db,
                dependencies.databaseManagementService(),
                dependencies.log().getUserLog(Uuid.class),
                dependencies.apocConfig(),
                dependencies.globalProceduresRegistry());
        
        // todo - forse cambiarlo e mettere getDbListeners(dependencies)
        final DataVirtualizationCatalogHandler dvHandler = new DataVirtualizationCatalogHandler(db, dependencies.log().getUserLog(DataVirtualizationCatalog.class));
        lists = List.of(uuidHandler, dvHandler);
        
        return MapUtil.genericMap(

                "ttl", new TTLLifeCycle(dependencies.scheduler(), db, dependencies.apocConfig(), dependencies.ttlConfig(), dependencies.log().getUserLog(TTLLifeCycle.class)),

                "uuid", uuidHandler,

                "directory", new LoadDirectoryHandler(db,
                        dependencies.log().getUserLog(LoadDirectory.class),
                        dependencies.pools()),

                "cypherProcedures", cypherProcedureHandler
                 , "dvHandler", dvHandler
//                , "dataVirtualizationAvailabilityHandler", new DataVirtualizationAvailabilityHandler(dependencies.databaseManagementService())
        );
    }

    @Override
    public Collection<Class> getContextClasses() {
        return List.of(CypherProceduresHandler.class, UuidHandler.class, LoadDirectoryHandler.class, DataVirtualizationCatalogHandler.class);
    }

    @Override
    public Iterable<AvailabilityListener> getListeners(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) { // todo - credo qua...
        // todo - valutare, credo non serva...
        CypherProceduresHandler cypherProceduresHandler = cypherProcedureHandlers.get(db);
        return cypherProceduresHandler==null ? Collections.emptyList() : List.of(cypherProceduresHandler);
    }

    @Override
    public List<DatabaseEventListener> getDbListeners() { // todo - credo qua...
        return lists;
//        return List.of();
//        CypherProceduresHandler cypherProceduresHandler = cypherProcedureHandlers.get(db);
//        return cypherProceduresHandler==null ? Collections.emptyList() : List.of(cypherProceduresHandler);
    }
}
