package apoc;

import apoc.util.ExtendedTestContainerUtil;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestContainerUtil.Neo4jVersion;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static apoc.util.TestContainerUtil.ApocPackage.CORE;
import static apoc.util.TestContainerUtil.ApocPackage.EXTENDED;
import static apoc.util.TestContainerUtil.createDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 This test is just to verify if the APOC procedures and functions are correctly deployed into a Neo4j instance without any startup issue.
 */
public class StartupExtendedTest {
    private static final String APOC_HELP_QUERY = "CALL apoc.help('') YIELD core, type, name WHERE core = $core and type = $type RETURN name";

    private static final List<String> EXPECTED_EXTENDED_PROC_NAMES = new ArrayList<>(ApocSignaturesExtended.PROCEDURES);

    static {
        // add Kotlin Procedures, not detected by ApocProcessor.java
        EXPECTED_EXTENDED_PROC_NAMES.addAll(List.of(
                "apoc.nlp.aws.entities.graph",
                "apoc.nlp.aws.entities.stream",
                "apoc.nlp.aws.keyPhrases.graph",
                "apoc.nlp.aws.keyPhrases.stream",
                "apoc.nlp.aws.sentiment.graph",
                "apoc.nlp.aws.sentiment.stream",
                "apoc.nlp.azure.entities.graph",
                "apoc.nlp.azure.entities.stream",
                "apoc.nlp.azure.keyPhrases.graph",
                "apoc.nlp.azure.keyPhrases.stream",
                "apoc.nlp.azure.sentiment.graph",
                "apoc.nlp.azure.sentiment.stream",
                "apoc.nlp.gcp.classify.graph",
                "apoc.nlp.gcp.classify.stream",
                "apoc.nlp.gcp.entities.graph",
                "apoc.nlp.gcp.entities.stream"
        ));
    }

    @Test
    public void checkCoreAndFullWithExtraDependenciesJars() {
        // we check that with apoc-extended, apoc-core jar and all extra-dependencies jars every procedure/function is detected
        // and that `extended.txt` (used by apoc.help procedure), contains with the procedures actually present
        startContainerSessionWithExtraDeps((version) -> createDB(version, List.of(CORE, EXTENDED), true),
                session -> {
                    checkCoreProcsAndFuncsExistence(session);

                    // all full procedures and functions are present, also the ones which require extra-deps, e.g. the apoc.export.xls.*
                    final List<String> functionExtNames = getNames(session, APOC_HELP_QUERY,
                            Map.of("core", false, "type", "function") );
                    final List<String> procExtNames = getNames(session, APOC_HELP_QUERY,
                            Map.of("core", false, "type", "procedure") );

                    assertEquals(sorted(ApocSignaturesExtended.FUNCTIONS), functionExtNames);
                    assertEquals(sorted(EXPECTED_EXTENDED_PROC_NAMES), procExtNames);
                });
    }

    @Test
    public void checkExtendedWithExtraDependenciesJars() {
        // we check that with apoc-extended jar and all extra-dependencies jars every procedure/function is detected
        startContainerSessionWithExtraDeps((version) -> createDB(version, List.of(EXTENDED), true),
                session -> {
                    // all full procedures and functions are present, also the ones which require extra-deps, e.g. the apoc.export.xls.*
                    final List<String> procExtNames = getNames(session, "SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc.' RETURN name");
                    final List<String> functionExtNames = getNames(session, "SHOW FUNCTIONS YIELD name WHERE name STARTS WITH 'apoc.' RETURN name");

                    assertEquals(sorted(ApocSignaturesExtended.FUNCTIONS), functionExtNames);
                    assertEquals(sorted(EXPECTED_EXTENDED_PROC_NAMES), procExtNames);
                });
    }

    @Test
    public void checkCoreWithExtraDependenciesJars() {
        // we check that with apoc-core jar and all extra-dependencies jars every procedure/function is detected
        startContainerSessionWithExtraDeps((version) -> createDB(version, List.of(CORE), true),
                this::checkCoreProcsAndFuncsExistence);
    }

    private void startContainerSessionWithExtraDeps(Function<Neo4jVersion, Neo4jContainerExtension> neo4jContainerCreation,
                                                    Consumer<Session> sessionConsumer) {
        for (var version: Neo4jVersion.values()) {

            try (final Neo4jContainerExtension neo4jContainer = neo4jContainerCreation.apply(version)) {
                // add extra-deps before starting it
                ExtendedTestContainerUtil.addExtraDependencies();
                neo4jContainer.start();
                assertTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());

                final Session session = neo4jContainer.getSession();

                sessionConsumer.accept(session);
            } catch (Exception ex) {
                // if Testcontainers wasn't able to retrieve the docker image we ignore the test
                if (TestContainerUtil.isDockerImageAvailable(ex)) {
                    ex.printStackTrace();
                    fail("Should not have thrown exception when trying to start Neo4j: " + ex);
                } else {
                    fail("The docker image could not be loaded. Check whether it's available locally / in the CI. Exception:" + ex);
                }
            }
        }
    }

    private void checkCoreProcsAndFuncsExistence(Session session) {
        final List<String> functionNames = getNames(session, APOC_HELP_QUERY,
                Map.of("core", true, "type", "function") );

        final List<String> procedureNames = getNames(session, APOC_HELP_QUERY,
                Map.of("core", true, "type", "procedure") );

        assertEquals(sorted(ApocSignatures.PROCEDURES), procedureNames);
        assertEquals(sorted(ApocSignatures.FUNCTIONS), functionNames);
    }

    private static List<String> getNames(Session session, String query, Map<String, Object> params) {
        return session.run(query, params)
                .list(i -> i.get("name").asString());
    }

    private static List<String> getNames(Session session, String query) {
        return getNames(session, query, Collections.emptyMap());
    }

    private List<String> sorted(List<String> signatures) {
        return signatures.stream()
                .sorted()
                .collect(Collectors.toList());
    }
}