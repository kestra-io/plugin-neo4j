package io.kestra.plugin.neo4j;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.neo4j.models.StoreType;
import io.kestra.plugin.neo4j.models.TrustStrategy;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests against a Neo4j container serving Bolt over TLS with a
 * private CA. The server certificate ({@code tls/bolt}) is signed by the test
 * CA ({@code tls/ca.crt}) and carries {@code localhost} in its SANs so the
 * driver hostname verification passes.
 */
@KestraTest
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Neo4jTlsIntegrationTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Container
    private static final Neo4jContainer<?> tlsContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:4.4"))
        .withEnv("NEO4J_dbms_connector_bolt_tls__level", "REQUIRED")
        .withEnv("NEO4J_dbms_ssl_policy_bolt_enabled", "true")
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("tls/bolt/private.key"),
            "/var/lib/neo4j/certificates/bolt/private.key"
        )
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("tls/bolt/public.crt"),
            "/var/lib/neo4j/certificates/bolt/public.crt"
        )
        .waitingFor(Wait.forLogMessage(".*Bolt enabled on.*", 1));

    @Test
    void tlsWithCustomCaSucceeds() throws Exception {
        Query query = baseQueryBuilder()
            .encryption(Property.ofValue(true))
            .trustedCertificate(Property.ofValue(testCaPem()))
            .storeType(Property.ofValue(StoreType.FETCHONE))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        Map<String, Object> row = run.getRow();
        assertThat(row.get("status"), is("tls-ok"));
    }

    @Test
    void tlsWithStorageCertificateSucceeds() throws Exception {
        URI uri;
        try (InputStream input = new FileInputStream(caCertificatePath().toFile())) {
            uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".crt"), input);
        }

        Query query = baseQueryBuilder()
            .encryption(Property.ofValue(true))
            .trustedCertificate(Property.ofValue(uri.toString()))
            .storeType(Property.ofValue(StoreType.FETCHONE))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        assertThat(run.getRow().get("status"), is("tls-ok"));
    }

    @Test
    void tlsWithSystemTrustFails() {
        Query query = baseQueryBuilder()
            .encryption(Property.ofValue(true))
            .trustStrategy(Property.ofValue(TrustStrategy.SYSTEM))
            .storeType(Property.ofValue(StoreType.FETCHONE))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());

        // The test CA is unknown to the system store, so the handshake must fail.
        assertThrows(ServiceUnavailableException.class, () -> query.run(runContext));
    }

    @Test
    void plaintextFailsWhenTlsIsRequired() {
        Query query = baseQueryBuilder()
            .encryption(Property.ofValue(false))
            .storeType(Property.ofValue(StoreType.FETCHONE))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());

        // The server requires TLS, so a plaintext connection must fail.
        assertThrows(ServiceUnavailableException.class, () -> query.run(runContext));
    }

    private static Query.QueryBuilder<?, ?> baseQueryBuilder() {
        return Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .url(Property.ofValue(tlsContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(tlsContainer.getAdminPassword()))
            .query(Property.ofValue("RETURN 'tls-ok' AS status"));
    }

    private static java.nio.file.Path caCertificatePath() throws Exception {
        return Paths.get(Neo4jTlsIntegrationTest.class.getClassLoader().getResource("tls/ca.crt").toURI());
    }

    private static String testCaPem() throws Exception {
        return Files.readString(caCertificatePath(), StandardCharsets.UTF_8);
    }
}
