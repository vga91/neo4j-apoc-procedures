package apoc;

import apoc.cypher.CypherInitializer;
import apoc.periodic.PeriodicCommitHandler;
import apoc.trigger.TriggerHandler;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@ServiceProvider
public class CoreApocGlobalComponents implements ApocGlobalComponents {

    @Override
    public Map<String,Lifecycle> getServices(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {
        return Map.of("trigger", new TriggerHandler(db,
                dependencies.databaseManagementService(),
                dependencies.apocConfig(),
                dependencies.log().getUserLog(TriggerHandler.class),
                dependencies.globalProceduresRegistry(),
                dependencies.pools()),
                "periodicCommitHandler", new PeriodicCommitHandler(db, dependencies.databaseManagementService())
        );
    }

    @Override
    public Collection<Class> getContextClasses() {
        return List.of(TriggerHandler.class, PeriodicCommitHandler.class);
    }

    @Override
    public Iterable<AvailabilityListener> getListeners(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {
        return Collections.singleton(new CypherInitializer(db, dependencies.log().getUserLog(CypherInitializer.class)));
    }
}
