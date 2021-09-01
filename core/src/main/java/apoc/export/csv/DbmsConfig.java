package apoc.export.csv;

import apoc.result.ProgressInfo;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import java.util.Map;
import java.util.stream.Stream;

public class DbmsConfig {

    @Context
    public GraphDatabaseAPI api;

    @UserFunction
    public String get(@Name("configKey") String configKey) throws Exception {
        final Config config = api.getDependencyResolver().resolveDependency(Config.class);
//        config.getSetting(configKey).name();
        final Setting<Object> setting = config.getSetting(configKey);
        return config.get(setting).toString();
    }
}
