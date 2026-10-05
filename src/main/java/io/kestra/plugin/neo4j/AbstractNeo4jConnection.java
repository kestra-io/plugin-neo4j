package io.kestra.plugin.neo4j;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.neo4j.models.AccessMode;
import io.kestra.plugin.neo4j.models.TrustStrategy;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@NoArgsConstructor
@Getter
public abstract class AbstractNeo4jConnection extends Task implements Neo4jConnectionInterface {
    public static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(30);
    public static final int DEFAULT_MAX_CONNECTION_POOL_SIZE = 100;

    private Property<String> url;

    @Schema(
        title = "Username for basic auth",
        description = "Must be used together with `password`."
    )
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> username;

    @Schema(
        title = "Password for basic auth",
        description = "Must be used together with `username`."
    )
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> password;

    @Schema(
        title = "Bearer token",
        description = "Base64-encoded bearer token. Cannot be combined with any other authentication mode."
    )
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> bearerToken;

    @Schema(
        title = "Kerberos ticket",
        description = "Base64-encoded Kerberos service ticket. Cannot be combined with any other authentication mode."
    )
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> kerberosTicket;

    @Schema(
        title = "Custom authentication scheme",
        description = "Scheme name for custom authentication. Must be set together with `customAuthPrincipal` and `customAuthCredentials`, and cannot be combined with any other authentication mode."
    )
    @PluginProperty(group = "connection")
    private Property<String> customAuthScheme;

    @Schema(
        title = "Custom authentication principal",
        description = "Principal for custom authentication. Must be set together with `customAuthScheme` and `customAuthCredentials`."
    )
    @PluginProperty(group = "connection")
    private Property<String> customAuthPrincipal;

    @Schema(
        title = "Custom authentication credentials",
        description = "Credentials for custom authentication. Must be set together with `customAuthScheme` and `customAuthPrincipal`."
    )
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> customAuthCredentials;

    @Schema(
        title = "Enable TLS encryption",
        description = "When `true`, encrypted traffic is forced with `withEncryption()`; when `false`, unencrypted traffic is forced with `withoutEncryption()`. " +
            "When unset, the driver default applies so the connection URI scheme keeps working (`bolt+s`, `bolt+ssc`, `neo4j+s`, `neo4j+ssc` stay encrypted, plain `bolt`/`neo4j` stay unencrypted), "
            +
            "except that supplying `trustStrategy` or `trustedCertificate` with a plain `bolt://` or `neo4j://` URL enables encryption automatically since trust settings are meaningless over plaintext."
    )
    @PluginProperty(group = "connection")
    private Property<Boolean> encryption;

    @Schema(
        title = "TLS trust strategy",
        description = "`SYSTEM` trusts system CA certificates (default), `CUSTOM` trusts the CA given in `trustedCertificate`, `ALL` trusts every certificate blindly and must only be used for development or tests. "
            +
            "When unset and `trustedCertificate` is supplied, `CUSTOM` is inferred; otherwise `SYSTEM` applies."
    )
    @PluginProperty(group = "connection")
    private Property<TrustStrategy> trustStrategy;

    @Schema(
        title = "Trusted CA certificate",
        description = "PEM-encoded CA certificate used with the `CUSTOM` trust strategy. Accepts either the PEM content itself (e.g. `\"{{ secret('NEO4J_CA_PEM') }}\"`) or a Kestra internal storage URI (e.g. `kestra://.../ca.crt`)."
    )
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> trustedCertificate;

    @Schema(
        title = "Connection timeout",
        description = "Maximum time to establish a socket connection (default 30 seconds)."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Duration> connectionTimeout = Property.ofValue(DEFAULT_CONNECTION_TIMEOUT);

    @Schema(
        title = "Maximum connection pool size",
        description = "Maximum number of pooled connections towards a single database, or per cluster member with routing (default 100)."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Integer> maxConnectionPoolSize = Property.ofValue(DEFAULT_MAX_CONNECTION_POOL_SIZE);

    @Schema(
        title = "Session access mode",
        description = "Routes units of work to read or write servers in a cluster. Ignored against a single server (default WRITE)."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<AccessMode> accessMode = Property.ofValue(AccessMode.WRITE);

    protected AuthToken credentials(RunContext runContext) throws IllegalVariableEvaluationException {
        return buildAuthToken(
            renderedOrNull(runContext, username),
            renderedOrNull(runContext, password),
            renderedOrNull(runContext, bearerToken),
            renderedOrNull(runContext, kerberosTicket),
            renderedOrNull(runContext, customAuthScheme),
            renderedOrNull(runContext, customAuthPrincipal),
            renderedOrNull(runContext, customAuthCredentials)
        );
    }

    static AuthToken buildAuthToken(
        String username,
        String password,
        String bearerToken,
        String kerberosTicket,
        String customAuthScheme,
        String customAuthPrincipal,
        String customAuthCredentials) {
        boolean basicConfigured = isNotBlank(username) || isNotBlank(password);
        boolean bearerConfigured = isNotBlank(bearerToken);
        boolean kerberosConfigured = isNotBlank(kerberosTicket);
        boolean customConfigured = isNotBlank(customAuthScheme) || isNotBlank(customAuthPrincipal) || isNotBlank(customAuthCredentials);

        if (basicConfigured && (!isNotBlank(username) || !isNotBlank(password))) {
            throw new IllegalArgumentException("Invalid Neo4j authentication: `username` and `password` must be configured together.");
        }

        if (
            customConfigured
                && (!isNotBlank(customAuthScheme) || !isNotBlank(customAuthPrincipal) || !isNotBlank(customAuthCredentials))
        ) {
            throw new IllegalArgumentException(
                "Invalid Neo4j authentication: `customAuthScheme`, `customAuthPrincipal` and `customAuthCredentials` must all be configured together."
            );
        }

        List<String> modes = new ArrayList<>();
        if (basicConfigured) {
            modes.add("basic (`username`/`password`)");
        }
        if (bearerConfigured) {
            modes.add("bearer (`bearerToken`)");
        }
        if (kerberosConfigured) {
            modes.add("kerberos (`kerberosTicket`)");
        }
        if (customConfigured) {
            modes.add("custom (`customAuthScheme`/`customAuthPrincipal`/`customAuthCredentials`)");
        }

        if (modes.size() > 1) {
            throw new IllegalArgumentException(
                "Invalid Neo4j authentication: conflicting authentication modes configured (" + String.join(", ", modes) + "). Configure exactly one of them."
            );
        }

        if (bearerConfigured) {
            return AuthTokens.bearer(bearerToken);
        }

        if (kerberosConfigured) {
            return AuthTokens.kerberos(kerberosTicket);
        }

        if (customConfigured) {
            return AuthTokens.custom(customAuthPrincipal, customAuthCredentials, null, customAuthScheme);
        }

        if (basicConfigured) {
            return AuthTokens.basic(username, password);
        }

        return AuthTokens.none();
    }

    protected Config driverConfig(RunContext runContext) throws Exception {
        Config.ConfigBuilder builder = Config.builder();

        Boolean encryption = runContext.render(this.encryption).as(Boolean.class).orElse(null);
        TrustStrategy trustStrategy = runContext.render(this.trustStrategy).as(TrustStrategy.class).orElse(null);
        String certificate = renderedOrNull(runContext, this.trustedCertificate);
        boolean trustConfigured = trustStrategy != null || certificate != null;

        if (Boolean.FALSE.equals(encryption) && trustConfigured) {
            throw new IllegalArgumentException(
                "Invalid Neo4j TLS configuration: `trustStrategy`/`trustedCertificate` require TLS encryption, but `encryption` is explicitly disabled. " +
                    "Enable `encryption` or remove the trust configuration."
            );
        }

        if (Boolean.TRUE.equals(encryption)) {
            builder.withEncryption();
        } else if (Boolean.FALSE.equals(encryption)) {
            builder.withoutEncryption();
        } else if (trustConfigured && isPlainScheme(renderedOrNull(runContext, getUrl()))) {
            // Trust settings are meaningless over plaintext: infer TLS instead of silently ignoring them.
            runContext.logger().debug("Enabling Neo4j TLS encryption because trust settings were supplied with a plain `bolt://`/`neo4j://` URL.");
            builder.withEncryption();
        }

        Duration timeout = runContext.render(this.connectionTimeout).as(Duration.class).orElse(DEFAULT_CONNECTION_TIMEOUT);
        builder.withConnectionTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);

        Integer poolSize = runContext.render(this.maxConnectionPoolSize).as(Integer.class).orElse(DEFAULT_MAX_CONNECTION_POOL_SIZE);
        builder.withMaxConnectionPoolSize(poolSize);

        if (certificate != null && trustStrategy != null && trustStrategy != TrustStrategy.CUSTOM) {
            throw new IllegalArgumentException(
                "Invalid Neo4j TLS configuration: `trustedCertificate` requires `trustStrategy` CUSTOM, but " + trustStrategy + " was configured."
            );
        }

        if (trustStrategy == null) {
            trustStrategy = certificate != null ? TrustStrategy.CUSTOM : TrustStrategy.SYSTEM;
        }

        File certificateFile = null;
        if (trustStrategy == TrustStrategy.CUSTOM) {
            if (certificate == null) {
                throw new IllegalArgumentException("Invalid Neo4j TLS configuration: `trustStrategy` CUSTOM requires `trustedCertificate` to be set.");
            }
            certificateFile = materializeCertificate(runContext, certificate);
        }

        builder.withTrustStrategy(buildTrustStrategy(trustStrategy, certificateFile));

        return builder.build();
    }

    static Config.TrustStrategy buildTrustStrategy(TrustStrategy trustStrategy, File certificateFile) {
        return switch (trustStrategy) {
            case SYSTEM -> Config.TrustStrategy.trustSystemCertificates();
            case ALL -> Config.TrustStrategy.trustAllCertificates();
            case CUSTOM -> Config.TrustStrategy.trustCustomCertificateSignedBy(certificateFile);
        };
    }

    /**
     * Materializes a rendered {@code trustedCertificate} value into a temporary PEM file.
     * The value is either inline PEM content or a Kestra internal storage URI.
     * The file lives in the task working directory (cleaned up with it) so it stays
     * available while the driver needs it. Certificate content is never logged.
     */
    protected File materializeCertificate(RunContext runContext, String certificate) throws Exception {
        byte[] bytes;
        String source;
        String value = certificate.strip();
        if (value.startsWith("kestra://")) {
            try (InputStream input = runContext.storage().getFile(URI.create(value))) {
                bytes = input.readAllBytes();
            }
            source = "internal storage";
        } else {
            bytes = certificate.getBytes(StandardCharsets.UTF_8);
            source = "inline PEM content";
        }

        if (bytes.length == 0) {
            throw new IllegalArgumentException("Invalid Neo4j TLS configuration: `trustedCertificate` is empty.");
        }

        Path tempFile = runContext.workingDir().createTempFile(".pem");
        Files.write(tempFile, bytes);
        runContext.logger().debug("Using custom Neo4j CA certificate from {} ({} bytes)", source, bytes.length);

        return tempFile.toFile();
    }

    protected SessionConfig sessionConfig(RunContext runContext) throws IllegalVariableEvaluationException {
        AccessMode mode = runContext.render(this.accessMode).as(AccessMode.class).orElse(AccessMode.WRITE);
        return buildSessionConfig(mode);
    }

    static SessionConfig buildSessionConfig(AccessMode accessMode) {
        return SessionConfig.builder()
            .withDefaultAccessMode(toDriverAccessMode(accessMode))
            .build();
    }

    static org.neo4j.driver.AccessMode toDriverAccessMode(AccessMode accessMode) {
        AccessMode mode = accessMode == null ? AccessMode.WRITE : accessMode;
        return switch (mode) {
            case READ -> org.neo4j.driver.AccessMode.READ;
            case WRITE -> org.neo4j.driver.AccessMode.WRITE;
        };
    }

    protected Driver buildDriver(RunContext runContext) throws Exception {
        String url = runContext.render(getUrl()).as(String.class).orElseThrow(() -> new IllegalArgumentException("Missing required Neo4j connection `url`."));
        return GraphDatabase.driver(url, credentials(runContext), driverConfig(runContext));
    }

    protected Session openSession(Driver driver, RunContext runContext) throws IllegalVariableEvaluationException {
        return driver.session(sessionConfig(runContext));
    }

    static boolean isPlainScheme(String url) {
        if (url == null) {
            return false;
        }
        String normalized = url.strip().toLowerCase(java.util.Locale.ROOT);
        int end = normalized.indexOf("://");
        if (end < 0) {
            return false;
        }
        String scheme = normalized.substring(0, end);
        return scheme.equals("bolt") || scheme.equals("neo4j");
    }

    private static String renderedOrNull(RunContext runContext, Property<String> property) throws IllegalVariableEvaluationException {
        if (property == null) {
            return null;
        }
        String rendered = runContext.render(property).as(String.class).orElse(null);
        return isNotBlank(rendered) ? rendered : null;
    }

    private static boolean isNotBlank(String value) {
        return value != null && !value.isBlank();
    }
}
