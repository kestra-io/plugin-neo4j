package io.kestra.plugin.neo4j;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.Config;
import org.neo4j.driver.SessionConfig;

import io.kestra.plugin.neo4j.models.AccessMode;
import io.kestra.plugin.neo4j.models.TrustStrategy;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class Neo4jDriverConfigTest {
    @Test
    void systemTrustStrategy() {
        Config.TrustStrategy strategy = AbstractNeo4jConnection.buildTrustStrategy(TrustStrategy.SYSTEM, null);
        assertThat(strategy.strategy(), is(Config.TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES));
    }

    @Test
    void allTrustStrategy() {
        Config.TrustStrategy strategy = AbstractNeo4jConnection.buildTrustStrategy(TrustStrategy.ALL, null);
        assertThat(strategy.strategy(), is(Config.TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES));
    }

    @Test
    void customTrustStrategyWithCertificateFile() {
        File certificate = testCaCertificate();
        Config.TrustStrategy strategy = AbstractNeo4jConnection.buildTrustStrategy(TrustStrategy.CUSTOM, certificate);
        assertThat(strategy.strategy(), is(Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES));
        assertThat(strategy.certFiles().size(), is(1));
    }

    @Test
    void plainSchemeDetection() {
        assertThat(AbstractNeo4jConnection.isPlainScheme("bolt://localhost:7687"), is(true));
        assertThat(AbstractNeo4jConnection.isPlainScheme("neo4j://localhost:7687"), is(true));
        assertThat(AbstractNeo4jConnection.isPlainScheme("BOLT://localhost:7687"), is(true));
        assertThat(AbstractNeo4jConnection.isPlainScheme("bolt+s://localhost:7687"), is(false));
        assertThat(AbstractNeo4jConnection.isPlainScheme("bolt+ssc://localhost:7687"), is(false));
        assertThat(AbstractNeo4jConnection.isPlainScheme("neo4j+s://localhost:7687"), is(false));
        assertThat(AbstractNeo4jConnection.isPlainScheme("neo4j+ssc://localhost:7687"), is(false));
        assertThat(AbstractNeo4jConnection.isPlainScheme(null), is(false));
        assertThat(AbstractNeo4jConnection.isPlainScheme("not-a-url"), is(false));
    }

    @Test
    void accessModeMappingDefaultsToWrite() {
        assertThat(AbstractNeo4jConnection.toDriverAccessMode(null), is(org.neo4j.driver.AccessMode.WRITE));
        assertThat(AbstractNeo4jConnection.toDriverAccessMode(AccessMode.WRITE), is(org.neo4j.driver.AccessMode.WRITE));
        assertThat(AbstractNeo4jConnection.toDriverAccessMode(AccessMode.READ), is(org.neo4j.driver.AccessMode.READ));
    }

    @Test
    void sessionConfigDefaultsToWrite() {
        SessionConfig sessionConfig = AbstractNeo4jConnection.buildSessionConfig(AccessMode.WRITE);
        assertThat(sessionConfig.defaultAccessMode(), is(org.neo4j.driver.AccessMode.WRITE));
    }

    @Test
    void sessionConfigSupportsRead() {
        SessionConfig sessionConfig = AbstractNeo4jConnection.buildSessionConfig(AccessMode.READ);
        assertThat(sessionConfig.defaultAccessMode(), is(org.neo4j.driver.AccessMode.READ));
    }

    static File testCaCertificate() {
        try {
            URL resource = Neo4jDriverConfigTest.class.getClassLoader().getResource("tls/ca.crt");
            assertThat("test CA certificate resource must exist", resource, org.hamcrest.Matchers.notNullValue());
            return Paths.get(resource.toURI()).toFile();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to load test CA certificate", e);
        }
    }
}
