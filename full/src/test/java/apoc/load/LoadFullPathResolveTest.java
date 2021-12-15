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

import static apoc.load.LoadFullSecurityTest.APOC_PROCEDURE_WITH_ARGUMENTS;

@RunWith(Parameterized.class)
public class LoadFullPathResolveTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.allow_file_urls, true)
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, 
                    Paths.get(ClassLoader.getSystemResource("test pipe column with spaces in filename.csv").getFile()).getParent());


    @BeforeClass
    public static void setUp() throws Exception {
        ApocConfig.apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(db, LoadXls.class, LoadHtml.class, LoadCsv.class);
    }

    private static final List<String> PREFIXES = List.of("", "file:/", "file://", "file:///", "subDir/", "file:/subDir/", "file://subDir/", "file:///subDir/");
    private final String apocProcedure;
    private final String fileName;

    public LoadFullPathResolveTest(String exportMethod, String exportMethodArguments, String fileName) {
        this.apocProcedure = "apoc.load." + exportMethod + exportMethodArguments;
        this.fileName = fileName;
    }

    private static final Map<String, String> fileNames = Map.of(
            "xls", "load test.xlsx",
            "html", "wiki pedia.html",
            "csv", "test pipe column with spaces in filename.csv",
            "csvParams", "test pipe column with spaces in filename.csv");

    @Parameterized.Parameters
    public static Collection<String[]> data() {
        return APOC_PROCEDURE_WITH_ARGUMENTS.entrySet().stream()
                .flatMap(x -> PREFIXES.stream().map(y -> new String[]{x.getKey(), x.getValue().get(0), y + fileNames.get(x.getKey())}))
                .collect(Collectors.toList());
    }

    @Test
    public void testIllegalFSAccessWithImportDisabled() {
        db.executeTransactionally("CALL " + apocProcedure,
                Map.of("fileName", fileName.startsWith("file") ? fileName.replace(" ", "%20") : fileName),
                Result::resultAsString);
    }
}
