package apoc.load;


import apoc.util.TestUtil;
import com.novell.ldap.LDAPEntry;
import com.novell.ldap.LDAPSearchResults;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.zapodot.junit.ldap.EmbeddedLdapRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.load.LoadLdapCommonTest.LDAP_HOST;
import static apoc.load.LoadLdapCommonTest.LDAP_PASSWORD;
import static apoc.load.LoadLdapCommonTest.SEARCH_MAP;
import static apoc.load.LoadLdapCommonTest.buildLdapRule;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class LoadLdapTest {
    private static final String LOGIN_DN = "CN=testuser,OU=Systemusers,OU=Users,dc=example,dc=com";
    private static final String LDAP_KEY = "myldap";

    @Rule
    public EmbeddedLdapRule embeddedLdapRule = buildLdapRule(LOGIN_DN);
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();
    
    @BeforeClass
    public static void setUp() {
        apocConfig().setProperty("apoc.loadldap." + LDAP_KEY + ".config", String.format("%s %s %s", LDAP_HOST, LOGIN_DN, LDAP_PASSWORD));
        TestUtil.registerProcedure(db, LoadLdap.class);
    }
    
    @Test
    public void testLoadLdapProcedureWithAuthKeyString() {
        testCall(db, "CALL apoc.load.ldap($key, $searchMap) YIELD entry RETURN entry.dn as dn,  entry.uniqueMember as uniqueMember",
                Map.of("key", LDAP_KEY, "searchMap", SEARCH_MAP), 
                LoadLdapCommonTest::ldapAssertions);
    }

    @Test
    public void testLoadLdapProcedureWithAuthMap() {
        testCall(db, "CALL apoc.load.ldap($conn, $searchMap) YIELD entry RETURN entry.dn as dn,  entry.uniqueMember as uniqueMember",
                Map.of("conn", Map.of("ldapHost", LDAP_HOST, "loginDN", LOGIN_DN, "loginPW", LDAP_PASSWORD), 
                        "searchMap", SEARCH_MAP),
                LoadLdapCommonTest::ldapAssertions);
    }

    @Test
    public void testLoadLDAP() throws Exception {
        Map<String, Object> connParms = new HashMap<>();
        connParms.put("ldapHost", "ldap.forumsys.com");
        connParms.put("ldapPort", 389l);
        connParms.put("loginDN", "cn=read-only-admin,dc=example,dc=com");
        connParms.put("loginPW", "password");
        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(LoadLdap.getConnectionMap(connParms));
        Map<String, Object> searchParms = new HashMap<>();
        searchParms.put("searchBase", "dc=example,dc=com");
        searchParms.put("searchScope", "SCOPE_ONE");
        searchParms.put("searchFilter", "(&(objectClass=*)(uid=training))");
        ArrayList<String> ats = new ArrayList<>();
        ats.add("uid");
        searchParms.put("attributes", ats);
        LDAPSearchResults results = mgr.doSearch(searchParms);
        LDAPEntry le = results.next();
        assertEquals("uid=training,dc=example,dc=com", le.getDN());
        assertEquals("training", le.getAttribute("uid").getStringValue());
    }

}

