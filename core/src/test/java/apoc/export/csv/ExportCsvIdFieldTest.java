package apoc.export.csv;

import apoc.ApocSettings;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static apoc.export.csv.ExportCsvNeo4jAdminTest.assertFileEquals;
import static apoc.util.MapUtil.map;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class ExportCsvIdFieldTest {
    
    private static final File directory = new File("target/import");
    
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath())
            .withSetting(ApocSettings.apoc_export_file_enabled, true);

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, ExportCSV.class);
    }

    @Test
    public void testExportCypherWithIdField() {
        // given
        final Map<String, Object> map = db.executeTransactionally("CREATE p=(source:User:Larus{id: 1, name: 'Andrea'})-[:KNOWS{id: 10}]->(target:User:Neo4j{id: 2, name: 'Michael'})\n" +
                "RETURN id(source) as sourceId, id(target) as targetId, p", Collections.emptyMap(), result ->  Iterators.single(result) );
        final String fileName = "export_id_field";
        String fileNameWithExtension = fileName + ".csv";
        File dir = new File(directory, fileNameWithExtension);

        // when
        TestUtil.testCall(db, "CALL apoc.export.csv.all($fileNameWithExtension,{bulkImport: true})",
                map("fileNameWithExtension", fileNameWithExtension, "p", map.get("p")), r -> {
                    // then
                    assertEquals(20000L, r.get("batchSize"));
                    assertEquals(1L, r.get("batches"));
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(3L, r.get("rows"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(5L, r.get("properties"));
                    assertTrue("Should get time greater than 0",
                            ((long) r.get("time")) >= 0);

                    String file = dir.getParent() + File.separator;
                    String expectedNodesLarus = String.format(":ID,name,id:long,:LABEL%n"
                            + "%s,Andrea,1,User;Larus%n", map.get("sourceId"));
                    String expectedNodesNeo4j = String.format(":ID,name,id:long,:LABEL%n"
                            +"%s,Michael,2,User;Neo4j%n", map.get("targetId"));
                    String expectedRelsNeo4j = String.format(":START_ID,:END_ID,:TYPE,id:long%n"
                            + "%s,%s,KNOWS,10%n", map.get("sourceId"), map.get("targetId"));

                    assertFileEquals(file, expectedNodesLarus, fileName + ".nodes.User.Larus.csv");
                    assertFileEquals(file, expectedNodesNeo4j, fileName + ".nodes.User.Neo4j.csv");
                    assertFileEquals(file, expectedRelsNeo4j, fileName + ".relationships.KNOWS.csv");
                }
        );
    }
}
