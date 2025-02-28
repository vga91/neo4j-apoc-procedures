package apoc.load;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URL;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LoadOffsetTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();
            //.withSetting(GraphDatabaseSettings.load_csv_file_url_root, Paths.get(getUrlFileName("test.csv").toURI()).getParent());

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadOffset.class);
        
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
        
        assertEquals("Rana,11\nSelina,", output);
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
    public void testLoadJsonZip() {
        URL url = getUrlFileName("testload.zip");
        testCall(db, "CALL apoc.load.stringPartial($url, 17, 15)", map("url", url.getPath() + "!person.json"), (row) -> {
            String string = row.toString();
            System.out.println("string = " + string);
        });
    }
}
