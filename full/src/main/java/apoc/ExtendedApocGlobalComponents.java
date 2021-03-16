package apoc;

import apoc.custom.CypherProcedures;
import apoc.custom.CypherProceduresHandler;
import apoc.sequence.SequenceHandler;
//import apoc.sequence.SequenceStorage;
import apoc.ttl.TTLLifeCycle;
import apoc.uuid.Uuid;
import apoc.uuid.UuidHandler;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@ServiceProvider
public class ExtendedApocGlobalComponents implements ApocGlobalComponents {

    private final Map<GraphDatabaseService,CypherProceduresHandler> cypherProcedureHandlers = new ConcurrentHashMap<>();
    private final Map<GraphDatabaseService, SequenceHandler> sequenceHandlers = new ConcurrentHashMap<>();

//    public static Map<GraphDatabaseService, SequenceHandler> storageHandler = new ConcurrentHashMap<>();

    @Override
    public Map<String, Lifecycle> getServices(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {


        CypherProceduresHandler cypherProcedureHandler = new CypherProceduresHandler(
                db,
                dependencies.scheduler(),
                dependencies.apocConfig(),
                dependencies.log().getUserLog(CypherProcedures.class),
                dependencies.globalProceduresRegistry()
        );
        cypherProcedureHandlers.put(db, cypherProcedureHandler);

        SequenceHandler sequenceHandler = new SequenceHandler(dependencies.apocConfig(), dependencies.scheduler(), db);

        sequenceHandlers.put(db, sequenceHandler);

        return MapUtil.genericMap(

                "ttl", new TTLLifeCycle(dependencies.scheduler(), db, dependencies.apocConfig(), dependencies.ttlConfig(), dependencies.log().getUserLog(TTLLifeCycle.class)),

                "uuid", new UuidHandler(db,
                dependencies.databaseManagementService(),
                dependencies.log().getUserLog(Uuid.class),
                dependencies.apocConfig(),
                dependencies.globalProceduresRegistry()),

                "cypherProcedures", cypherProcedureHandler,
                "sequence", sequenceHandler
        );
    }

    @Override
    public Collection<Class> getContextClasses() {
        return List.of(CypherProceduresHandler.class, UuidHandler.class, SequenceHandler.class);
    }

    @Override
    public Iterable<AvailabilityListener> getListeners(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {
        CypherProceduresHandler cypherProceduresHandler = cypherProcedureHandlers.get(db);
        SequenceHandler sequenceHandler = sequenceHandlers.get(db);
        Set<AvailabilityListener> listeners = new HashSet<>();
        if (sequenceHandler != null) {
            listeners.add(sequenceHandler);
        }
        if (cypherProceduresHandler != null) {
            listeners.add(cypherProceduresHandler);
        }
        return listeners;
//        return cypherProceduresHandler==null ? Collections.emptyList() : Collections.singleton(cypherProceduresHandler);
    }
}
