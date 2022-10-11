package apoc;

import apoc.help.HelpExtendedTest;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import apoc.util.TestContainerUtil.ApocPackage;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 This test is just to verify if the APOC are correctly deployed
 into a Neo4j instance without any startup issue.
 If you don't have docker installed it will fail, and you can simply ignore it.
 */
public class CoreExtendedTest extends HelpExtendedTest {
    @Test
    public void checkForCoreAndExtended() {
        try {
            Neo4jContainerExtension neo4jContainer = createEnterpriseDB(List.of(/*ApocPackage.CORE, */ApocPackage.EXTENDED), true)
                    .withNeo4jConfig("dbms.transaction.timeout", "60s");
//                    .withNeo4jConfig(APOC_IMPORT_FILE_ENABLED, "true");

            neo4jContainer.start();

            
            Session session = neo4jContainer.getSession();
            // todo - common with StartupTest.compare_with_sources (in core)
//            final List<String> functionNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = true and type = 'function' RETURN name")
//                    .list(record -> record.get("name").asString());
//            final List<String> procedureNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = true and type = 'procedure' RETURN name")
//                    .list(record -> record.get("name").asString());
//
//
//            assertEquals(sorted(ApocSignatures.PROCEDURES), procedureNames);
//            assertEquals(sorted(ApocSignatures.FUNCTIONS), functionNames);
            final List<String> totalExtendedNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = false and type = 'procedure' RETURN name")
                    .list(record -> record.get("name").asString());
            final List<String> functionExtendedNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = false and type = 'function' RETURN name")
                    .list(record -> record.get("name").asString());

            totalExtendedNames.addAll(functionExtendedNames);
            final String actualExtended = totalExtendedNames.stream()
                    .collect(Collectors.joining("\n"));

            final String expectedExtended = FileUtils.readFileToString(EXTENDED_FILE, StandardCharsets.UTF_8);
            assertEquals(expectedExtended, actualExtended);

            neo4jContainer.close();
        } catch (Exception ex) {
            if (TestContainerUtil.isDockerImageAvailable(ex)) {
                ex.printStackTrace();
                fail("Should not have thrown exception when trying to start Neo4j: " + ex);
            }
        }
    }


    private List<String> sorted(List<String> signatures) {
        return signatures.stream().sorted().collect(Collectors.toList());
    }

    // TODO [Nacho] Ignored for the moment because we cannot build core from here anymore. This needs rethinking
    @Test
    public void matchesSpreadsheet() {
        try {
            Neo4jContainerExtension neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE, TestContainerUtil.ApocPackage.EXTENDED), true)
                    .withNeo4jConfig("dbms.transaction.timeout", "60s");

            neo4jContainer.start();

            Session session = neo4jContainer.getSession();

            Result result = session.run("load csv with headers from 'file:///apoc-core-extended.csv' AS row RETURN row.Name as Name, row.Decision AS Decision");

            Map<String, String> spreadsheet = new HashMap<>();
            List<Record> list = result.list();
            for (Record record : list) {
                spreadsheet.put(record.get("Name").asString(), record.get("Decision").asString());
            }

            Map<String, String> actual = new HashMap<>();
            Result apocHelpResult = session.run("CALL apoc.help('')");
            for (Record record : apocHelpResult.list()) {
                actual.put(record.get("name").toString(), record.get("core").asBoolean() ? "CORE" : "EXTENDED");
            }

            List<Map.Entry<String, String>> different = spreadsheet.entrySet().stream().filter(entry -> actual.containsKey(entry.getKey()) && !actual.get(entry.getKey()).equals(entry.getValue())).collect(Collectors.toList());

            assertEquals(different.toString(), 0, different.size());

            neo4jContainer.close();
        } catch (Exception ex) {
            if (TestContainerUtil.isDockerImageAvailable(ex)) {
                ex.printStackTrace();
                fail("Should not have thrown exception when trying to start Neo4j: " + ex);
            }
        }
    }
}
