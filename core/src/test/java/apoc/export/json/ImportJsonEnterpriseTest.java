package apoc.export.json;

import apoc.util.CompressionAlgo;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.io.File;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.export.json.JsonImporter.MISSING_CONSTRAINT_ERROR_MSG;
import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.Util.map;
import static java.lang.String.format;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class ImportJsonEnterpriseTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() throws Exception {
        TestUtil.ignoreException(() -> {
            // We build the project, the artifact will be placed into ./build/libs
            neo4jContainer = createEnterpriseDB(!TestUtil.isRunningInCI())
                    .withEnv(APOC_IMPORT_FILE_ENABLED, "true");
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        assumeTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        if (neo4jContainer != null && neo4jContainer.isRunning()) {
            session.close();
            neo4jContainer.close();
        }
    }

    @Test
    public void shouldFailsDueToMissingUniqueConstraint() {
        final byte[] file = fileToBinary(new File(ImportJsonTest.directory, "all.json"), CompressionAlgo.GZIP.name());

        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT ON (n:User) assert n.neo4jImportId IS NOT NULL");
            tx.commit();
            return null;
        });

        // when
        try {
            testCall(session, "CALL apoc.import.json($file, $config)",
                    map("file", file, "config", map(COMPRESSION, CompressionAlgo.GZIP.name())),
                    r -> fail("Should fail due to missing constraint"));
        } catch (Exception e) {
            String expectedMsg = format(MISSING_CONSTRAINT_ERROR_MSG, "User", "neo4jImportId");
            assertTrue(e.getMessage().contains(expectedMsg));
        }

    }
}
