package apoc.load.util;

import apoc.load.LoadImportConfig;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Map;

/**
 * @author ab-Larus
 * @since 03-10-18
 */
public class LoadJdbcConfig extends LoadImportConfig {

    private Credentials credentials;

    private final Long fetchSize;

    private final boolean autoCommit;

    public LoadJdbcConfig(Map<String,Object> config) {
        // to preserve previous behaviour we leverage on java.sql.Timestamp (see Jdbc.ResultSetIterator.convert() method)
        //  and not on db.temporal.timezone config (as other load procedures) 
        super(config, null);
        config = config != null ? config : Collections.emptyMap();
        this.credentials = config.containsKey("credentials") ? createCredentials((Map<String, String>) config.get("credentials")) : null;
        this.fetchSize = Util.toLong(config.getOrDefault("fetchSize", 5000L));
        this.autoCommit = Util.toBoolean(config.getOrDefault("autoCommit", false));
    }

    @Override
    public Object createMapping(Object input) {
        // mapping not necessary
        return null; 
    }

    public Credentials getCredentials() {
        return this.credentials;
    }

    public static Credentials createCredentials(Map<String,String> credentials) {
        if (!credentials.getOrDefault("user", StringUtils.EMPTY).equals(StringUtils.EMPTY) && !credentials.getOrDefault("password", StringUtils.EMPTY).equals(StringUtils.EMPTY)) {
            return new Credentials(credentials.get("user"), credentials.get("password"));
        } else {
            throw new IllegalArgumentException("In config param credentials must be passed both user and password.");
        }
    }

    public static class Credentials {
        private String user;

        private String password;

        public Credentials(String user, String password){
            this.user = user;

            this.password = password;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }
    }

    public boolean hasCredentials() {
        return this.credentials != null;
    }

    public Long getFetchSize() {
        return fetchSize;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }
}