package apoc.load;

import org.zapodot.junit.ldap.EmbeddedLdapRule;
import org.zapodot.junit.ldap.EmbeddedLdapRuleBuilder;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LoadLdapCommonTest {
    private static final int LDAP_PORT = 12345;
    
    static final String LDAP_HOST = "127.0.0.1:" + LDAP_PORT;
    static final String LDAP_PASSWORD = "myPassword";
    static final Map<String, String> SEARCH_MAP = Map.of("searchBase", "dc=example1,dc=com", "searchScope", "SCOPE_SUB", "searchFilter", "(&(objectClass=*domain))");

    
    static EmbeddedLdapRule buildLdapRule(String loginDn) {
        return EmbeddedLdapRuleBuilder
                .newInstance()
                .usingDomainDsn("dc=example1,dc=com")
                .importingLdifs("example.ldif")
                .usingBindDSN(loginDn)
                .usingBindCredentials(LDAP_PASSWORD)
                .bindingToPort(LDAP_PORT)
                .build();
    }

    static void ldapAssertions(Map<String, Object> r) {
        assertEquals("ou=Users,dc=example1,dc=com", r.get("dn"));
        assertEquals("foobar", r.get("uniqueMember"));
    }
}
