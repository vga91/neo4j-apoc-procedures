package apoc.load;

import apoc.ApocConfig;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.load.LoadCoreSecurityTest.APOC_PROCEDURE_WITH_ARGUMENTS;

@RunWith(Parameterized.class)
public class LoadCorePathResolveTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.allow_file_urls, true)
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, Paths.get(ClassLoader.getSystemResource("multi with spaces.json.json").getFile()).getParent());


    @BeforeClass
    public static void setUp() throws Exception {
        ApocConfig.apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(db, LoadJson.class, Xml.class);
    }
    
    private static final List<String> PREFIXES = List.of("", "file:/", "file://", "file:///", "subDir/", "file:/subDir/", "file://subDir/", "file:///subDir/");
    private final String apocProcedure;
    private final String fileName;

    public LoadCorePathResolveTest(String exportMethod, String exportMethodArguments, String fileName) {
        this.apocProcedure = "apoc.load." + exportMethod + exportMethodArguments;
        this.fileName = fileName;
    }
    
    private static final Map<String, String> fileNames = Map.of(
            "json", "multi with spaces.json",
            "jsonArray", "multi with spaces.json",
            "jsonParams", "multi with spaces.json",
            "xml", "file with spaces.xml",
            "xmlSimple", "file with spaces.xml");

    @Parameterized.Parameters
    public static Collection<String[]> data() {
        return APOC_PROCEDURE_WITH_ARGUMENTS.entrySet().stream()
                .flatMap(x -> PREFIXES.stream().map(y -> new String[]{x.getKey(), x.getValue().get(0), y + fileNames.get(x.getKey())}))
                .collect(Collectors.toList());
    }

    @Test
    public void testIllegalFSAccessWithImportDisabled() {
        db.executeTransactionally("CALL " + apocProcedure,
                    Map.of("fileName", fileName),
                    Result::resultAsString);
    }
    
}
