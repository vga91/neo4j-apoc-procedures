package apoc.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.gradle.tooling.BuildLauncher;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static apoc.util.TestUtil.printFullStackTrace;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestContainerUtil {

    public static final String PASSWORD = "apoc";
    public static final String DOCKER_ENTERPRISE_IMAGE = "neo4j:4.2.2-enterprise";
    public static final long TIMEOUT = 300 * 1_000;

    private TestContainerUtil() {}

    private static File baseDir = Paths.get(".").toFile();

    public static Neo4jContainer initializeEnterpriseDb(Neo4jContainer container) {
        return initializeEnterpriseDb(container, !TestUtil.isRunningInCI());
    }

    public static Neo4jContainer initializeEnterpriseDb(Neo4jContainer container, boolean withLogging) {
        executeGradleTasks("shadowJar");
        // We define the container with external volumes
        File importFolder = new File("import");
        importFolder.mkdirs();

        // read neo4j version from build.gradle and use this as default
        String neo4jDockerImageVersion = System.getProperty("neo4jDockerImage", DOCKER_ENTERPRISE_IMAGE);

        // use a separate folder for mounting plugins jar - build/libs might contain other jars as well.
        File pluginsFolder = new File("build/plugins");
        pluginsFolder.mkdirs();

        Collection<File> files = FileUtils.listFiles(new File("build/libs"), new WildcardFileFilter(Arrays.asList("*-all.jar", "*-core.jar")), null);
        for (File file: files) {
            try {
                FileUtils.copyFileToDirectory(file, pluginsFolder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        String canonicalPath = null;
        try {
            canonicalPath = importFolder.getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("neo4jDockerImageVersion = " + neo4jDockerImageVersion);

        boolean containerNull = container == null;
        Neo4jContainer<?> initContainer = containerNull
                ? new Neo4jContainerExtension(neo4jDockerImageVersion).withAdminPassword(PASSWORD)
                : (Neo4jContainer<?>) container.withNeo4jConfig("causal_clustering.leadership_balancing", "NO_BALANCING")
                                                .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes");
        
        Neo4jContainer neo4jContainer = initContainer
                .withPlugins(MountableFile.forHostPath(pluginsFolder.toPath()))
                .withEnv("NEO4J_dbms_memory_heap_max__size", "512M")
                .withEnv("NEO4J_dbms_memory_pagecache_size", "256M")
                .withEnv("apoc.export.file.enabled", "true")
                .withNeo4jConfig("dbms.security.procedures.unrestricted", "apoc.*")
                .withFileSystemBind(canonicalPath, "/var/lib/neo4j/import") // map the "target/import" dir as the Neo4j's import dir
//                .withDebugger()  // uncomment this line for remote debbuging inside docker's neo4j instance
                .withCreateContainerCmdModifier(cmd -> cmd.withMemory(1024 * 1024 * 1024l))

                // set uid if possible - export tests do write to "/import"
                .withCreateContainerCmdModifier(cmd -> {
                    try {
                        Process p = Runtime.getRuntime().exec("id -u");
                        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        String s = br.readLine();
                        p.waitFor();
                        p.destroy();
                        cmd.withUser(s);
                    } catch (Exception e) {
                        System.out.println("Exception while assign cmd user to docker container:\n" + ExceptionUtils.getStackTrace(e));
                        // ignore since it may fail depending on operating system
                    }
                });
        if (withLogging) {
            neo4jContainer.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(
                    containerNull ? Neo4jContainerExtension.class : Neo4jCluster.class)));
        }
        return neo4jContainer;
    }

    public static Neo4jContainerExtension createEnterpriseDB(boolean withLogging)  {
        return (Neo4jContainerExtension) initializeEnterpriseDb(null, withLogging);
    }

    public static void executeGradleTasks(String... tasks) {
        try (ProjectConnection connection = GradleConnector.newConnector()
                .forProjectDirectory(baseDir)
                .useBuildDistribution()
                .connect()) {
//            String version = connection.getModel(ProjectPublications.class).getPublications().getAt(0).getId().getVersion();

            BuildLauncher buildLauncher = connection.newBuild().forTasks(tasks);

            String neo4jVersionOverride = System.getenv("NEO4JVERSION");
            System.out.println("neo4jVersionOverride = " + neo4jVersionOverride);
            if (neo4jVersionOverride != null) {
                buildLauncher = buildLauncher.addArguments("-P", "neo4jVersionOverride=" + neo4jVersionOverride);
            }

            String localMaven = System.getenv("LOCAL_MAVEN");
            System.out.println("localMaven = " + localMaven);
            if (localMaven != null) {
                buildLauncher = buildLauncher.addArguments("-D", "maven.repo.local=" + localMaven);
            }

            buildLauncher.run();
        }
    }

    public static void testCall(Session session, String call, Map<String,Object> params, Consumer<Map<String, Object>> consumer) {
        testResult(session, call, params, (res) -> {
            try {
                assertNotNull("result should be not null", res);
                assertTrue("result should be not empty", res.hasNext());
                Map<String, Object> row = res.next();
                consumer.accept(row);
                assertFalse("result should not have next", res.hasNext());
            } catch(Throwable t) {
                printFullStackTrace(t);
                throw t;
            }
        });
    }

    public static void testCall(Session session, String call, Consumer<Map<String, Object>> consumer) {
        testCall(session, call, null, consumer);
    }

    public static void testResult(Session session, String call, Consumer<Iterator<Map<String, Object>>> resultConsumer) {
        testResult(session, call, null, resultConsumer);
    }

    public static void testResult(Session session, String call, Map<String,Object> params, Consumer<Iterator<Map<String, Object>>> resultConsumer) {
        session.writeTransaction(tx -> {
            Map<String, Object> p = (params == null) ? Collections.<String, Object>emptyMap() : params;
            resultConsumer.accept(tx.run(call, p).list().stream().map(Record::asMap).collect(Collectors.toList()).iterator());
            tx.commit();
            return null;
        });
    }

    public static void testCallInReadTransaction(Session session, String call, Consumer<Map<String, Object>> consumer) {
        testCallInReadTransaction(session, call, null, consumer);
    }

    public static void testCallInReadTransaction(Session session, String call, Map<String,Object> params, Consumer<Map<String, Object>> consumer) {
        testResultInReadTransaction(session, call, params, (res) -> {
            try {
                assertNotNull("result should be not null", res);
                assertTrue("result should be not empty", res.hasNext());
                Map<String, Object> row = res.next();
                consumer.accept(row);
                assertFalse("result should not have next", res.hasNext());
            } catch(Throwable t) {
                printFullStackTrace(t);
                throw t;
            }
        });
    }

    public static void testResultInReadTransaction(Session session, String call, Consumer<Iterator<Map<String, Object>>> resultConsumer) {
        testResultInReadTransaction(session, call, null, resultConsumer);
    }

    public static void testResultInReadTransaction(Session session, String call, Map<String,Object> params, Consumer<Iterator<Map<String, Object>>> resultConsumer) {
        session.readTransaction(tx -> {
            Map<String, Object> p = (params == null) ? Collections.<String, Object>emptyMap() : params;
            resultConsumer.accept(tx.run(call, p).list().stream().map(Record::asMap).collect(Collectors.toList()).iterator());
            tx.commit();
            return null;
        });
    }

}
