package apoc.load;


import apoc.util.FileUtils;
import apoc.util.TestUtil;
import com.novell.ldap.LDAPEntry;
import com.novell.ldap.LDAPSearchResults;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustAllTrustManager;
import org.apache.commons.net.util.SSLContextUtils;
import org.apache.commons.net.util.SSLSocketUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.zapodot.junit.ldap.EmbeddedLdapRule;
import org.zapodot.junit.ldap.EmbeddedLdapRuleBuilder;

import javax.naming.Context;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * $ docker run -p 389:389 -p 636:636 --name my-openldap-container --volume ./ldif:/container/service/slapd/assets/config/bootstrap/ldif/custom --detach osixia/openldap:1.5.0  --copy-service
 * docker cp 478ceb6c78c8e296d2a68d652828a4a75b9edec2201d8d8533762b2ae7f77f2e:/container/service/slapd/assets/certs .
 * 
 * 
 * $ docker run -p 389:389 -p 636:636 --name my-openldap-container --volume ./ldif:/container/service/slapd/assets/config/bootstrap/ldif/custom --env LDAP_TLS_VERIFY_CLIENT=try --detach osixia/openldap:1.5.0  --copy-service
 * 
 * 
 * $ docker run -p 389:389 -p 636:636 --name my-openldap-container --volume ./path:/container/service/slapd/assets/certs --detach osixia/openldap:1.5.0  --copy-service
 * 
 * docker run -p 389:389 -p 636:636 --name my-openldap-container --hostname ldap.my-company.com --detach osixia/openldap:1.5.0 --copy-service
 * 
 * 
 * 
 *

 ldapsearch -x -H ldaps://localhost:636 -b dc=example,dc=org -D "cn=admin,dc=example,dc=org" -w admin
 ldapsearch -x -H ldap://localhost:389 -b dc=example,dc=org -D "cn=admin,dc=example,dc=org" -w admin


 ——
 https://docs.servicenow.com/bundle/washingtondc-platform-security/page/administer/general/task/t_GenerateAnLDAPClientCertificate.html
 openssl s_client -connect localhost:636 -showcerts 
 openssl s_client -connect localhost:636 -CAfile ./dhparam.pem 
 
 
 TODOOOOO - dico che con docker ssl sembra tricky, e sul web le soluzioni sono "disattiva certificati" o "just use ldap://..."
 // -- ldapsearch -W -H ldaps://ldap.forumsys.com:636 -D "uid=tesla,dc=example,dc=com" -b "dc=example,dc=com"


 -- https://support.google.com/a/answer/9190869?hl=en
 
 */
public class LoadLdapTest {
    public static final String BIND_DSN = "uid=admin,cn=users,cn=accounts,dc=demo1,dc=freeipa";
    public static final String BIND_PWD = "testPwd";
    public static LDAPConnection ldapConnection;
    
    public static Map<String, Object> connParams;
    public static Map<String, Object> searchParams;

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static GraphDatabaseService db;

    public static class UnsecuredSSLSocketFactory extends SSLSocketFactory
    {
        private SSLSocketFactory socketFactory;

        public UnsecuredSSLSocketFactory()
        {
            try
            {
                var sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, new TrustManager[]{new X509TrustManager()
                {
                    @Override
                    public void checkClientTrusted(X509Certificate[] xcs, String string){}

                    @Override
                    public void checkServerTrusted(X509Certificate[] xcs, String string){}

                    @Override
                    public X509Certificate[] getAcceptedIssuers()
                    {
                        return null;
                    }
                }}, new SecureRandom());
                socketFactory = sslContext.getSocketFactory();
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unused")
        public static SocketFactory getDefault()
        {
            return new UnsecuredSSLSocketFactory();
        }

        @Override
        public String[] getDefaultCipherSuites()
        {
            return socketFactory.getDefaultCipherSuites();
        }

        @Override
        public String[] getSupportedCipherSuites()
        {
            return socketFactory.getSupportedCipherSuites();
        }

        @Override
        public Socket createSocket(Socket socket, String string, int i, boolean bln) throws IOException
        {
            return socketFactory.createSocket(socket, string, i, bln);
        }

        @Override
        public Socket createSocket(String string, int i) throws IOException
        {
            return socketFactory.createSocket(string, i);
        }

        @Override
        public Socket createSocket(String string, int i, InetAddress ia, int i1) throws IOException
        {
            return socketFactory.createSocket(string, i, ia, i1);
        }

        @Override
        public Socket createSocket(InetAddress ia, int i) throws IOException
        {
            return socketFactory.createSocket(ia, i);
        }

        @Override
        public Socket createSocket(InetAddress ia, int i, InetAddress ia1, int i1) throws IOException
        {
            return socketFactory.createSocket(ia, i, ia1, i1);
        }

        @Override
        public Socket createSocket() throws IOException
        {
            return socketFactory.createSocket();
        }
    }

    @ClassRule
    public static EmbeddedLdapRule embeddedLdapRule;

    static {
        try {
//            SOCKET_FACTORY

//            SSLUtil sslUtil = new SSLUtil(new TrustAllTrustManager());
//            
//            SSLSocketFactory socketFactory = sslUtil.createSSLSocketFactory();
                    //SSLContext.getDefault().getSocketFactory();

//            env.put("java.naming.ldap.factory.socket", UnsecuredSSLSocketFactory.class.getName());


            embeddedLdapRule = EmbeddedLdapRuleBuilder
                .newInstance()
                .usingBindDSN(BIND_DSN)
                .usingBindCredentials(BIND_PWD)
                    
                .withSocketFactory(new UnsecuredSSLSocketFactory())
                    .useTls(true)
//                .withSocketFactory(socketFactory)
                .importingLdifs("ldap/example.ldif")
                .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        DatabaseManagementService dbms = new TestDatabaseManagementServiceBuilder(tempFolder.getRoot().toPath()).build();
        db = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        TestUtil.registerProcedure(db, LoadLdap.class);

        ldapConnection = embeddedLdapRule.unsharedLdapConnection();
//        Context context = embeddedLdapRule.context();

        connParams = Map.of("ldapHost", "localhost:" + ldapConnection.getConnectedPort(),
                "loginDN", BIND_DSN,
                "loginPW", BIND_PWD);

        searchParams = Map.of("searchBase", "dc=example,dc=com",
//                "searchScope", "SCOPE_SUBTREE",
                "searchScope", "SCOPE_ONE",
                "searchFilter", "(objectClass=*)",
                "attributes", List.of("uid") );
    }

    @AfterClass
    public static void afterClass()  {
        ldapConnection.close();
    }

    @Test
    public void testLoadLDAPWithApocConfig1() {
//        Map<String, Object> connParams1 = new HashMap<>(connParams);
//        connParams1.put("ldapHost", "localhost:636");
        int port = 636;
        extracted(port);
    }
    
    @Test
    public void testLoadLDAPWithApocConfig12() {
//        Map<String, Object> connParams1 = new HashMap<>(connParams);
//        connParams1.put("ldapHost", "localhost:636");
        int port = 389;
        extracted(port);
    }

    private static void extracted(int port) {
        Map<String, String> conn = Map.of("ldapHost", "localhost:" + port,
//        Map<String, String> conn = Map.of("ldapHost", "ldaps://localhost:" + port,
                "loginDN", "cn=admin,dc=example,dc=org",
                "loginPW", "admin");
//        Map<String, Object> searchBase = Map.of("searchBase", "dc=example,dc=com",
//                "searchScope", "SCOPE_BASE",
//                "searchFilter", "(objectclass=*)"/*,
//                "attributes", List.of("uid")*/);
        Map<String, Object> searchBase = Map.of("searchBase", "dc=example,dc=org",
                "searchScope", "SCOPE_BASE");
        testCall(db, "call apoc.load.ldap($conn, $search)",
                Map.of("conn", conn, "search", searchBase),
                r -> {
                    System.out.println("r = " + r);
                });

        // javax.naming.CommunicationException: simple bind failed: localhost:61178 [Root exception is javax.net.ssl.SSLException: Unsupported or unrecognized SSL message]
    }

    @Test
    public void testLoadLDAPWithApocConfig() {
        String key = "apoc.loadldap.myldap.config";
        testWithStringConfigCommon(key);

        // the config with dot after loadldap shouldn't print a log warn
        String logWarn = "Not to cause breaking-change, the current config `apoc.loadldap.myldap.config` is valid";
        assertFalse(getLogFileContent().contains(logWarn));
    }

    @Test
    public void testLoadLDAPWithApocConfigWithoutDotBeforeLdapKey() {
        // analogous to `testLoadLDAPWithApocConfig`, but without dot between `loadldap` and `myldap`
        // it still works not to cause a breaking change
        String key = "apoc.loadldapmyldap.config";
        testWithStringConfigCommon(key);

        // the config without dot after loadldap should print a log warn
        String logWarn = "Not to cause breaking-change, the current config `apoc.loadldapmyldap.config` is valid";
        assertTrue(getLogFileContent().contains(logWarn));
    }

    private static String getLogFileContent() {
        try {
            File logFile = new File(FileUtils.getLogDirectory(), "debug.log");
            return Files.readString(logFile.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void testWithStringConfigCommon(String key) {
        // set a config `key=localhost:port dns pwd`
        String ldapValue = "%s %s %s".formatted(
                "localhost:" + ldapConnection.getConnectedPort(),
                BIND_DSN,
                BIND_PWD);
        apocConfig().setProperty(key, ldapValue);

        testCall(db, "call apoc.load.ldap($conn, $search)",
                Map.of("conn", "myldap", "search", searchParams),
                this::testLoadAssertionCommon);

        // remove current config to prevent multiple confs in other tests
        apocConfig().getConfig().clearProperty(key);
    }

    @Test
    public void testLoadLDAPWithWrongApocConfig() {
        apocConfig().setProperty("apoc.loadldap.mykey.config", "host logindn pwd");

        String expected = "No apoc.loadldap.wrongKey.config ldap access configuration specified";
        try {
            testCall(db, "call apoc.load.ldap('wrongKey', {})",
                    r -> fail("Should fail due to: " + expected));
        } catch (RuntimeException e) {
            String actual = e.getMessage();
            assertTrue("Current err. message is: " + actual, actual.contains(expected));
        }
    }

    @Test
    public void testLoadLDAP() {
        testCall(db, "call apoc.load.ldap($conn, $search)",
                Map.of("conn", connParams, "search", searchParams),
                this::testLoadAssertionCommon);
    }

    private void testLoadAssertionCommon(Map<String, Object> r) {
        final Map<String, String> expected = Map.of("uid", "training",
                "dn", "uid=training,dc=example,dc=com");
        assertEquals(expected, r.get("entry"));
    }

//    @Test
//    public void testLoadLDAPConfig() throws Exception {
//        LoadLdap.LDAPManager mgr = new LoadLdap.LDAPManager(LoadLdap.getConnectionMap(connParams, null));
//        
//        LDAPSearchResults results = mgr.doSearch(searchParams);
//        LDAPEntry le = results.next();
//        assertEquals("uid=training,dc=example,dc=com", le.getDN());
//        assertEquals("training", le.getAttribute("uid").getStringValue());
//
//    }

}

