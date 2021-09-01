package apoc;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

/**
 * a kernel extension for the new apoc configuration mechanism
 */
@ServiceProvider
public class ApocConfigExtensionFactory extends ExtensionFactory<ApocConfigExtensionFactory.Dependencies> {

    public interface Dependencies {
        AvailabilityGuard availabilityGuard();
        LogService log();
        Config config();
        GlobalProcedures globalProceduresRegistry();
        DatabaseManagementService databaseManagementService();
        GraphDatabaseAPI graphdatabaseAPI();
    }

    public ApocConfigExtensionFactory() {
        super(ExtensionType.GLOBAL, "ApocConfig");
        System.out.println("ApocConfigExtensionFactory.ApocConfigExtensionFactory");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
//        GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
        System.out.println("ApocConfigExtensionFactory.newInstance");
        return new ApocConfig(dependencies.config(), dependencies.log(), dependencies.globalProceduresRegistry(), dependencies.databaseManagementService(), null);
    }

}
