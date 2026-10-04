package io.kestra.plugin.neo4j;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.SessionConfig;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.neo4j.models.AccessMode;
import io.kestra.plugin.neo4j.models.TrustStrategy;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Neo4jConnectionConfigTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void defaultDriverConfig() throws Exception {
        Query task = queryBuilder().build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        Config config = task.driverConfig(runContext);

        assertThat(config.encrypted(), is(false));
        assertThat(config.trustStrategy().strategy(), is(Config.TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES));
        assertThat(config.connectionTimeoutMillis(), is(30000));
        assertThat(config.maxConnectionPoolSize(), is(100));
    }

    @Test
    void defaultSessionConfigIsWrite() throws Exception {
        Query task = queryBuilder().build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        SessionConfig sessionConfig = task.sessionConfig(runContext);

        assertThat(sessionConfig.defaultAccessMode(), is(org.neo4j.driver.AccessMode.WRITE));
    }

    @Test
    void readSessionConfig() throws Exception {
        Query task = queryBuilder().accessMode(Property.ofValue(AccessMode.READ)).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        SessionConfig sessionConfig = task.sessionConfig(runContext);

        assertThat(sessionConfig.defaultAccessMode(), is(org.neo4j.driver.AccessMode.READ));
    }

    @Test
    void encryptionCanBeEnabled() throws Exception {
        Query task = queryBuilder().encryption(Property.ofValue(true)).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        assertThat(task.driverConfig(runContext).encrypted(), is(true));
    }

    @Test
    void trustAllStrategy() throws Exception {
        Query task = queryBuilder()
            .encryption(Property.ofValue(true))
            .trustStrategy(Property.ofValue(TrustStrategy.ALL))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        Config config = task.driverConfig(runContext);

        assertThat(config.encrypted(), is(true));
        assertThat(config.trustStrategy().strategy(), is(Config.TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES));
    }

    @Test
    void customTrustIsInferredFromCertificate() throws Exception {
        String pem = testCaPem();
        Query task = queryBuilder()
            .encryption(Property.ofValue(true))
            .trustedCertificate(Property.ofValue(pem))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        Config config = task.driverConfig(runContext);

        assertThat(config.trustStrategy().strategy(), is(Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES));
        assertThat(config.trustStrategy().certFiles().size(), is(1));
        String stored = Files.readString(config.trustStrategy().certFiles().get(0).toPath(), StandardCharsets.UTF_8);
        assertThat(stored, is(pem));
    }

    @Test
    void certificateFromInternalStorage() throws Exception {
        String pem = testCaPem();
        URI uri;
        try (InputStream input = new FileInputStream(caCertificatePath().toFile())) {
            uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".crt"), input);
        }

        Query task = queryBuilder()
            .encryption(Property.ofValue(true))
            .trustedCertificate(Property.ofValue(uri.toString()))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        Config config = task.driverConfig(runContext);

        assertThat(config.trustStrategy().strategy(), is(Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES));
        assertThat(config.trustStrategy().certFiles().size(), is(1));
        String stored = Files.readString(config.trustStrategy().certFiles().get(0).toPath(), StandardCharsets.UTF_8);
        assertThat(stored, is(pem));
    }

    @Test
    void systemTrustWithCertificateIsRejected() {
        Query task = queryBuilder()
            .trustStrategy(Property.ofValue(TrustStrategy.SYSTEM))
            .trustedCertificate(Property.ofValue("pem-content"))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.driverConfig(runContext));
        assertThat(e.getMessage(), containsString("trustedCertificate"));
    }

    @Test
    void customTrustWithoutCertificateIsRejected() {
        Query task = queryBuilder()
            .trustStrategy(Property.ofValue(TrustStrategy.CUSTOM))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.driverConfig(runContext));
        assertThat(e.getMessage(), containsString("trustedCertificate"));
    }

    @Test
    void customTimeoutAndPoolSize() throws Exception {
        Query task = queryBuilder()
            .connectionTimeout(Property.ofValue(Duration.ofSeconds(10)))
            .maxConnectionPoolSize(Property.ofValue(5))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        Config config = task.driverConfig(runContext);

        assertThat(config.connectionTimeoutMillis(), is(10000));
        assertThat(config.maxConnectionPoolSize(), is(5));
    }

    @Test
    void renderedCredentials() throws Exception {
        Query basic = queryBuilder()
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue("secret"))
            .build();
        assertThat(basic.credentials(TestsUtils.mockRunContext(runContextFactory, basic, ImmutableMap.of())), notNullValue());

        Query none = queryBuilder().build();
        AuthToken token = none.credentials(TestsUtils.mockRunContext(runContextFactory, none, ImmutableMap.of()));
        assertThat(token, notNullValue());

        Query conflicting = queryBuilder()
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue("secret"))
            .bearerToken(Property.ofValue("YmVhdG9rZW4="))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, conflicting, ImmutableMap.of());
        assertThrows(IllegalArgumentException.class, () -> conflicting.credentials(runContext));

        Query halfBasic = queryBuilder()
            .username(Property.ofValue("neo4j"))
            .build();
        RunContext halfContext = TestsUtils.mockRunContext(runContextFactory, halfBasic, ImmutableMap.of());
        assertThrows(IllegalArgumentException.class, () -> halfBasic.credentials(halfContext));
    }

    private static Query.QueryBuilder<?, ?> queryBuilder() {
        return Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .url(Property.ofValue("bolt://localhost:7687"))
            .query(Property.ofValue("RETURN 1 AS n"));
    }

    private static java.nio.file.Path caCertificatePath() throws Exception {
        return Paths.get(Neo4jConnectionConfigTest.class.getClassLoader().getResource("tls/ca.crt").toURI());
    }

    private static String testCaPem() throws Exception {
        return Files.readString(caCertificatePath(), StandardCharsets.UTF_8);
    }
}
