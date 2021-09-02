package apoc.load;

import apoc.cache.Static;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.zapodot.junit.ldap.EmbeddedLdapRule;

import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.load.LoadLdapCommonTest.LDAP_HOST;
import static apoc.load.LoadLdapCommonTest.LDAP_PASSWORD;
import static apoc.load.LoadLdapCommonTest.SEARCH_MAP;
import static apoc.load.LoadLdapCommonTest.buildLdapRule;
import static apoc.util.TestUtil.testCall;
import static java.util.Collections.singletonList;

public class LoadLdapStaticTest {
    private static final String LOGIN_DN_WITH_SPACES = "CN=testuser,OU=System users,OU=Users,dc=example,dc=com";

    @Rule
    public EmbeddedLdapRule embeddedLdapRule = buildLdapRule(LOGIN_DN_WITH_SPACES);

    @BeforeClass
    public static void setUp() {
        apocConfig().setProperty("apoc.static.ldap.host", LDAP_HOST);
        apocConfig().setProperty("apoc.static.ldap.loginDn", LOGIN_DN_WITH_SPACES);
        apocConfig().setProperty("apoc.static.ldap.password", LDAP_PASSWORD);
        TestUtil.registerProcedure(db, LoadLdap.class, Static.class);
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, singletonList("apoc.*"));


    @Test
    public void testLoadLdapProcedureWithStatics() {
        testCall(db, "CALL apoc.load.ldap({ldapHost : apoc.static.get('ldap.host'), loginDN : apoc.static.get('ldap.loginDn'), loginPW : apoc.static.get('ldap.password')}, $searchMap)\n" +
                        "YIELD entry RETURN entry.dn as dn,  entry.uniqueMember as uniqueMember",
                Map.of("searchMap", SEARCH_MAP),
                LoadLdapCommonTest::ldapAssertions);
    }
}
