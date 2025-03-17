package apoc.neo4j.docker;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CustomNewProcedureMultiDbTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Driver driver;
    private static Session neo4jSession;
    private static Session testSession;
    private static Session fooSession;
    private static Session barSession;

    private static final String DB_TEST = "dbtest";
    private static final String DB_FOO = "dbfoo";
    private static final String DB_BAR = "dbbar";

    @BeforeClass
    public static void setupContainer() {
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.EXTENDED), true)
                .withEnv(Map.of("apoc.ttl.enabled." + DB_TEST, "false",
                        "apoc.ttl.enabled", "true",
                        "apoc.ttl.schedule", "2",
                        "apoc.ttl.schedule." + DB_FOO, "7",
                        "apoc.ttl.limit", "200",
                        "apoc.ttl.limit." + DB_BAR, "2000"));
        neo4jContainer.start();
        driver = neo4jContainer.getDriver();
        createDatabases();
        createSessions();
    }

    @After
    public void cleanDb() {
        neo4jSession.executeWrite(tx -> tx.run("MATCH (n) DETACH DELETE n;").consume());
        testSession.executeWrite(tx -> tx.run("MATCH (n) DETACH DELETE n;").consume());
        fooSession.executeWrite(tx -> tx.run("MATCH (n) DETACH DELETE n;").consume());
        barSession.executeWrite(tx -> tx.run("MATCH (n) DETACH DELETE n;").consume());
    }

    @AfterClass
    public static void bringDownContainer() {
        neo4jContainer.close();
    }

    
    // TODO - like TTLMultiDbTest

    private static void createDatabases() {
        try(Session systemSession = driver.session(SessionConfig.forDatabase("system"))) {
            systemSession.executeWrite(tx -> {
                tx.run("CREATE DATABASE " + DB_TEST + " WAIT;").consume();
                tx.run("CREATE DATABASE " + DB_FOO + " WAIT;").consume();
                return tx.run("CREATE DATABASE " + DB_BAR + " WAIT;").consume();
            });
        }

        try(Session systemSession = driver.session(SessionConfig.forDatabase("system"))) {
            assertEventually(() -> {
                final List<Record> list = systemSession.run("SHOW DATABASES YIELD name, currentStatus")
                        .list();
                return list.stream().allMatch(i -> i.get("currentStatus").asString().equals("online"))
                        && list.stream().map(i -> i.get("name").asString()).toList().containsAll(List.of(DB_TEST , DB_FOO , DB_BAR));
            }, value -> value, 30L, TimeUnit.SECONDS);
        }
    }

    private static void createSessions() {
        neo4jSession = neo4jContainer.getSession();
        testSession = driver.session(SessionConfig.forDatabase(DB_TEST));
        fooSession = driver.session(SessionConfig.forDatabase(DB_FOO));
        barSession = driver.session(SessionConfig.forDatabase(DB_BAR));
    }

}
