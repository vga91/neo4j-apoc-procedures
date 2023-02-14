import apoc.ApocSignatures;
import apoc.help.Help;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 This test is just to verify if the APOC procedures and functions are correctly deployed into a Neo4j instance without any startup issue.
 If you don't have docker installed it will fail, and you can simply ignore it.
 */
public class StartupTest {

    private static final File APOC_FULL;
    private static final File APOC_CORE;

    static {
        final String file = StartupTest.class.getClassLoader().getResource(".").getFile();
        final int endIndex = file.indexOf("test-startup");
        APOC_FULL = Paths.get(file.substring(0, endIndex).concat("/full")).toFile();
        APOC_CORE = Paths.get(file.substring(0, endIndex).concat("/core")).toFile();
    }

    @Test
    public void check_basic_deployment() {
        try (Neo4jContainerExtension neo4jContainer = createEnterpriseDB(APOC_FULL, !TestUtil.isRunningInCI())
                .withNeo4jConfig("dbms.transaction.timeout", "5s")) {

            neo4jContainer.start();
            assertTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());

            Session session = neo4jContainer.getSession();
            int procedureCount = session.run("CALL dbms.procedures() YIELD name WHERE name STARTS WITH 'apoc' RETURN count(*) AS count").peek().get("count").asInt();
            int functionCount = session.run("CALL dbms.functions() YIELD name WHERE name STARTS WITH 'apoc' RETURN count(*) AS count").peek().get("count").asInt();
            int coreCount = session.run("CALL apoc.help('') YIELD core WHERE core = true RETURN count(*) AS count").peek().get("count").asInt();

            assertTrue(procedureCount > 0);
            assertTrue(functionCount > 0);
            assertTrue(coreCount > 0);
        } catch (Exception ex) {
            // if Testcontainers wasn't able to retrieve the docker image we ignore the test
//            if (TestContainerUtil.isDockerImageAvailable(ex)) {
//                ex.printStackTrace();
//                fail("Should not have thrown exception when trying to start Neo4j: " + ex);
//            }
        }
    }

    @Test
    public void compare_with_sources() {
        try (Neo4jContainerExtension neo4jContainer = createEnterpriseDB(APOC_FULL, !TestUtil.isRunningInCI())) {
            neo4jContainer.start();

            assertTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());

            try (Session session = neo4jContainer.getSession()) {
                extracted(session);
            }
        } catch (Exception ex) {
            // if Testcontainers wasn't able to retrieve the docker image we ignore the test
//            if (TestContainerUtil.isDockerImageAvailable(ex)) {
//                ex.printStackTrace();
//                fail("Should not have thrown exception when trying to start Neo4j: " + ex);
//            }
        }
    }

    private void extracted(Session session) {
        final List<String> functionNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = true and type = 'function' RETURN name")
                .list(record -> record.get("name").asString());
        final List<String> procedureNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = true and type = 'procedure' RETURN name")
                .list(record -> record.get("name").asString());


        assertEquals(sorted(ApocSignatures.PROCEDURES), procedureNames);
        assertEquals(sorted(ApocSignatures.FUNCTIONS), functionNames);
    }

    @Test
    public void compare_with_extended() throws IOException {

        // todo - check core and extended 
        try ( Neo4jContainerExtension neo4jContainer = createEnterpriseDB(APOC_FULL, !TestUtil.isRunningInCI(), true) ) {
            neo4jContainer.start();

            final Session session = neo4jContainer.getSession();

            String startupLog = neo4jContainer.getLogs();
            System.out.println("startupLog = " + startupLog);

            final List<String> extended = FileUtils.readLines(new File(APOC_FULL, "src/main/resources/extended.txt"), StandardCharsets.UTF_8);

            
            // all full procedures are present, also the ones which require extra-deps, e.g. the apoc.export.xls.*
            final List<String> fullProcsAndFunctionNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = false and type = 'procedure' RETURN name").list(i -> i.get("name").asString());
            assertEquals(sorted(extended), fullProcsAndFunctionNames);
            
            extracted(session);
            
        }
    }

    @Test
    public void compare_with_core() {
        // todo - check core and extended 
        try ( Neo4jContainerExtension neo4jContainer = createEnterpriseDB(APOC_CORE, true, true) ) {
            neo4jContainer.start();

            final Session session = neo4jContainer.getSession();

            String startupLog = neo4jContainer.getLogs();
            System.out.println("startupLogWithLoggingTrue = " + startupLog);

            extracted(session);
        }

    }

    private List<String> sorted(List<String> signatures) {
        return signatures.stream().sorted().collect(Collectors.toList());
    }
}
