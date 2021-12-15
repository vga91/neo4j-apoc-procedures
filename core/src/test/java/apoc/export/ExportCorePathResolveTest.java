package apoc.export;

import apoc.ApocConfig;
import apoc.ApocSettings;
import apoc.export.csv.ExportCSV;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.load.LoadJson;
import apoc.load.Xml;
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

import java.io.File;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.load.LoadCoreSecurityTest.APOC_PROCEDURE_WITH_ARGUMENTS;

@RunWith(Parameterized.class)
public class ExportCorePathResolveTest {

    private static File directory = new File("target/import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
        new File(directory, "subDir").mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath())
            .withSetting(ApocSettings.apoc_export_file_enabled, true);
    
    @BeforeClass
    public static void setUp() throws Exception {
        ApocConfig.apocConfig().setProperty(ApocConfig.APOC_EXPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(db, ExportCSV.class, ExportJson.class, ExportGraphML.class, ExportCypher.class);
    }

    private static final List<String> UNO = List.of("", "file:/", "file://", "file:///", "subDir/", "file:/subDir/", "file://subDir/", "file:///subDir/");
    private final String apocProcedure;
    private final String fileName;

    public ExportCorePathResolveTest(String exportMethod, String fileName) {
        this.apocProcedure = "apoc.export." + exportMethod + ".all($fileName, {})";
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
        return List.of("csv", "cypher", "graphml", "json").stream()
                .flatMap(x -> UNO.stream().map(y -> new String[]{x, y + "file name"}))
                .collect(Collectors.toList());
    }

    @Test
    public void testIllegalFSAccessWithImportDisabled() {
        db.executeTransactionally("CALL " + apocProcedure,
                Map.of("fileName", fileName),
                Result::resultAsString);
    }
}
