package apoc.text;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Collection;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

/**
 * @author Stefan Armbruster
 */
@RunWith(Parameterized.class)
public class StringCleanTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Strings.class);
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                { "&N[]eo  4 #J-(3.0)  ", "neo4j30", "&n[]eo  4 #j-(3.0)  ", "&n[]eo  4 #j-(3.0)  "},
                { "German umlaut Ä Ö Ü ä ö ü ß ", "germanumlautaeoeueaeoeuess", "german umlaut ae oe ue ae oe ue ss ", "german umlaut ae oe ue ae oe ue ss " },
                { "French çÇéèêëïîôœàâæùûüñ", "frenchcceeeeiioœaaæuuuen", "french cceeeeiioœaaæuuuen", "french ççéèêëïîôœàâæùûueñ"}
        });
    }

    @Parameter(value = 0)
    public String dirty;

    @Parameter(value = 1)
    public String clean;

    @Parameter(value = 2)
    public String clean2;

    @Parameter(value = 3)
    public String clean3;

    @Test
    public void testClean() throws Exception {
        testCall(db,
                "RETURN apoc.text.clean($a) AS value",
                map("a", dirty),
                row -> assertEquals(clean, row.get("value")));
    }

    @Test
    public void testCleanWithOnlyAnumFalse() {
        testCall(db,
                "RETURN apoc.text.clean($a, {onlyAnum: false}) AS value",
                map("a", dirty),
                row -> assertEquals(clean2, row.get("value")));
    }

    @Test
    public void testCleanWithOnlyAnumFalse2() {
        testCall(db,
                "RETURN apoc.text.clean($a, {onlyAnum: false, normalizerForm: 'NFKC'}) AS value",
                map("a", dirty),
                row -> assertEquals(clean3, row.get("value")));
    }

}
