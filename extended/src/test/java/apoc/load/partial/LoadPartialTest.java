package apoc.load.partial;

import apoc.util.TestUtil;
import apoc.util.Utils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URL;
import java.util.Map;

import static apoc.ApocConfig.*;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static org.junit.Assert.assertEquals;

public class LoadPartialTest {

    public static final String RANA_11_SELINA = "Rana,11\nSelina,";


    // TODO - s3 and gc tests??
    private static final String COMPLEX_STRING = "Mätrix II 哈哈\uD83D\uDE04123";
    private static final String COMPLEX_STRING_PARTIAL = COMPLEX_STRING.substring(4, 15);
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();
            //.withSetting(GraphDatabaseSettings.load_csv_file_url_root, Paths.get(getUrlFileName("test.csv").toURI()).getParent());

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadPartial.class, Utils.class);
        
        // TODO - check of this one!!
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        
    }
    
    /*
    Rana,11
Selina,18
     */
    
    // https://examplefile.com/file-download/559
    
    // https://www.kaggle.com/zanjibar/100-million-data-csv
    
    @Test
    public void testLoadPartialWithImportNotEnabled() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, false);

        URL urlFileName = getUrlFileName("test.csv");
        String path = urlFileName.getPath();
        
        assertFails(db, "CALL apoc.load.stringPartial($url, 17, 15)", Map.of("url", path),
                LOAD_FROM_FILE_ERROR);

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
    }
    
    @Test
    public void testLoadCsv() throws Exception {
        URL urlFileName = getUrlFileName("test.csv");
        String path = urlFileName.getPath();
        // String url = "test.csv";
        extracted(path);
    }

    @Test
    public void testLoadCsvByUrl() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/neo4j-contrib/neo4j-apoc-procedures/refs/heads/dev/extended/src/test/resources/test.csv");
        String path = url.toString();
        extracted(path);

    }
    
    // curl -O https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.csv

    // TODO --> https://www3.stats.govt.nz/2018census/Age-sex-by-ethnic-group-grouped-total-responses-census-usually-resident-population-counts-2006-2013-2018-Censuses-RC-TA-SA2-DHB.zip
    //      try with zips?
    
    @Test
    public void testLoadCsvLargeFile() throws Exception {
        // 30MB file
        URL urlFileName = new URL("https://www.stats.govt.nz/assets/Uploads/Balance-of-payments/Balance-of-payments-and-international-investment-position-September-2024-quarter/Download-data/balance-of-payments-and-international-investment-position-september-2024-quarter.csv");
        String path = urlFileName.toString();
        // String url = "test.csv";
        int limit = 300;
        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 50, $limit)",
                map("url", path, "limit", limit));

        System.out.println("output = " + output);
        assertEquals(limit, output.length());
//        System.out.println("output = " + output);
//        System.out.println("output = " + output.length());
    }

    @Test
    public void testLoadCsvLargeFileZip() throws Exception {
        
        // 100MB zip file, TODO - doens't decode. Use CompressionAlgo maybe???
        // 800MB csv file inside it
        URL urlFileName = new URL("https://www3.stats.govt.nz/2018census/Age-sex-by-ethnic-group-grouped-total-responses-census-usually-resident-population-counts-2006-2013-2018-Censuses-RC-TA-SA2-DHB.zip!Data8277.csv");
        String path = urlFileName.toString();
        // String url = "test.csv";
        int limit = 300;
        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 50, $limit)",
                map("url", path, "limit", limit));

        System.out.println("output = " + output);
        assertEquals(limit, output.length());
//        System.out.println("output = " + output);
//        System.out.println("output = " + output.length());
    }

    private void extracted(String path) {
        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)",
                map("url", path));
        
        assertEquals(RANA_11_SELINA, output);
    }
    
    
    // TODO --> @UserFunction("apoc.json.path") --> USARE JsonUtil.parse(json, path, Object.class, pathOptions); ??
    //  per fare risultato come lista di mappe simil json

    @Test public void testLoadCsvTarGzByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j/apoc/blob/dev/core/src/test/resources/testload.tar.gz?raw=true");
        testResult(db, "CALL apoc.load.stringPartial($url, 17, 15)", map("url",url.toString()+"!csv/test.csv"), // 'file:test.csv'
                (r) -> {
                    String s = r.resultAsString();
                    System.out.println("s = " + s);
//                    assertRow(r,0L,"name","Selma","age","8");
//                    assertRow(r,1L,"name","Rana","age","11");
//                    assertRow(r,2L,"name","Selina","age","18");
//                    assertEquals(false, r.hasNext());
                });
    }

    // todo - altri compression files
    
    // todo - zip e tar.gz locali
    @Test
    public void testLoadJsonTarGz() {
        URL url = getUrlFileName("testload.tar.gz");
        testCall(db, "CALL apoc.load.stringPartial($url, 17, 15)", map("url", url.getPath() + "!person.json"), (row) -> {
            String string = row.toString();
            System.out.println("string = " + string);
        });
    }
    
    @Test
    public void testLoadJsonTgz() {
        URL url = getUrlFileName("testload.tgz");
        testCall(db, "CALL apoc.load.stringPartial($url, 17, 15)", map("url", url.getPath() + "!person.json"), (row) -> {
            String string = row.toString();
            System.out.println("string = " + string);
        });
    }
    
    
    @Test
    public void testLoadJsonTar() {
        URL url = getUrlFileName("testload.tar");
        testCall(db, "CALL apoc.load.stringPartial($url, 17, 15)", map("url", url.getPath() + "!person.json"), (row) -> {
            String string = row.toString();
            System.out.println("string = " + string);
        });
    }

    @Test
    public void testLoadJsonZip() {
        URL url = getUrlFileName("testload.zip");
        testCall(db, "CALL apoc.load.stringPartial($url, 17, 15)", map("url", url.getPath() + "!person.json"), (row) -> {
            String string = row.toString();
            System.out.println("string = " + string);
        });
    }
    
    @Test
    public void testCompressAndDecompressWithMultipleCompressionAlgosReturningStartString() {

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'GZIP'}) AS compressed " +
                        "CALL apoc.load.stringPartial(compressed, 5, 17, {compression: 'GZIP'}) YIELD value RETURN value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING_PARTIAL, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'BZIP2'}) AS compressed " +
                        "CALL apoc.load.stringPartial(compressed, 5, 17, {compression: 'BZIP2'}) YIELD value RETURN value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING_PARTIAL, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'DEFLATE'}) AS compressed " +
                        "CALL apoc.load.stringPartial(compressed, 5, 17, {compression: 'DEFLATE'}) YIELD value RETURN value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING_PARTIAL, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'BLOCK_LZ4'}) AS compressed " +
                        "CALL apoc.load.stringPartial(compressed, 5, 17, {compression: 'BLOCK_LZ4'}) YIELD value RETURN value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING_PARTIAL, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'FRAMED_SNAPPY'}) AS compressed " +
                        "CALL apoc.load.stringPartial(compressed, 5, 17, {compression: 'FRAMED_SNAPPY'}) YIELD value RETURN value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING_PARTIAL, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'NONE'}) AS compressed " +
                        "CALL apoc.load.stringPartial(compressed, 5, 17, {compression: 'NONE'}) YIELD value RETURN value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING_PARTIAL, r.get("value")));
    }
}
