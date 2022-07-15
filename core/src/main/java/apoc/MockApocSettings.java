//package apoc;
//
//import org.neo4j.annotations.service.ServiceProvider;
//import org.neo4j.configuration.SettingsDeclaration;
//import org.neo4j.graphdb.config.Setting;
//
//import static apoc.util.SystemDbUtil.KEY_CURRENT_DB;
//import static org.neo4j.configuration.SettingImpl.newBuilder;
//import static org.neo4j.configuration.SettingValueParsers.BOOL;
//
//@ServiceProvider
//public class MockApocSettings implements SettingsDeclaration {
////    public MockApocSettings() { }
//
//    public static final Setting<Boolean> apoc_trigger_enabled2 = newBuilder(KEY_CURRENT_DB, BOOL, false).build();
//}