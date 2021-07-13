package apoc.util;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

public class TestcontainersCausalCluster {
    private Driver driver;
    private Session session;

    public TestcontainersCausalCluster(String uri, String username, String password) {
        AuthToken token = AuthTokens.basic(username, password);
        this.driver = GraphDatabase.driver(uri, token);
        this.session = driver.session();
    }

    public Driver getDriver() {
        return driver;
    }

    public Session getSession() {
        return session;
    }

    public void close() {
        getSession().close();
        getDriver().close();
    }
}
