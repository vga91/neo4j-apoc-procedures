package apoc.export.parquet;

import apoc.graph.Graphs;
import apoc.load.LoadParquet;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import apoc.util.s3.S3BaseTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.parquet.ParquetTest.MAPPING_ALL;
import static apoc.export.parquet.ParquetTest.MAPPING_QUERY;
import static apoc.export.parquet.ParquetTest.testReturnNodeAndRelCommon;
import static apoc.util.TestUtil.testResult;

public class ParquetS3Test extends S3BaseTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() {
        TestUtil.registerProcedure(db, ExportParquet.class, LoadParquet.class, ImportParquet.class, Graphs.class, Meta.class);
    }

    @Before
    public void before() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015-05-18T19:32:24.000'), place:point({latitude: 13.1, longitude: 33.46789, height: 100.0})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42})");
        db.executeTransactionally("CREATE (:Another {foo:1}), (:Another {bar:'Sam'})");

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @Test
    public void testFileRoundtripParquetAll() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.export.parquet.all('test_all.parquet') YIELD file",
                Map.of(),
                ParquetTest::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("file", file,  "config", MAPPING_ALL),
                ParquetTest::roundtripLoadAllAssertion);
    }

    @Test
    public void testReturnNodeAndRel() {
        testReturnNodeAndRelCommon(() -> db.executeTransactionally(
                "CALL apoc.export.parquet.query('MATCH (n:ParquetNode)-[r:BAR]->(o:Other) RETURN n,r,o ORDER BY n.idStart', " +
                "'volume_test.parquet', $config) YIELD file ",
                Map.of("config", MAPPING_QUERY),
                ParquetTest::extractFileName));
    }

}
