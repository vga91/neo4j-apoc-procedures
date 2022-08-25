package apoc;

import apoc.cypher.CypherInitializer;
import apoc.trigger.TriggerHandler;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@ServiceProvider
public class CoreApocGlobalComponents implements ApocGlobalComponents {
    private List<DatabaseEventListener> lists = new ArrayList<>();
    
    @Override
    public Map<String,Lifecycle> getServices(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {
        final TriggerHandler triggerHandler = new TriggerHandler(db,
                dependencies.databaseManagementService(),
                dependencies.apocConfig(),
                dependencies.log().getUserLog(TriggerHandler.class),
                dependencies.globalProceduresRegistry(),
                dependencies.pools(),
                dependencies.scheduler());
        lists = List.of(triggerHandler);
        return Collections.singletonMap("trigger", triggerHandler);
    }

    @Override
    public Collection<Class> getContextClasses() {
        return Collections.singleton(TriggerHandler.class);
    }

    @Override
    public Iterable<AvailabilityListener> getListeners(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {
        return Collections.singleton(new CypherInitializer(db, dependencies.log().getUserLog(CypherInitializer.class)));
    }

    @Override
    public List<DatabaseEventListener> getDbListeners() {
        return lists;
    }
}
