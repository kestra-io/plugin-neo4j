package io.kestra.plugin.neo4j.models;

/**
 * Plugin-level representation of the Neo4j driver TLS trust strategy.
 *
 * <ul>
 * <li>{@code SYSTEM} trusts certificates verifiable through the local system store.</li>
 * <li>{@code CUSTOM} trusts certificates signed by the custom CA given in {@code trustedCertificate}.</li>
 * <li>{@code ALL} trusts every certificate blindly; only use it for development or tests.</li>
 * </ul>
 */
public enum TrustStrategy {
    SYSTEM,
    CUSTOM,
    ALL
}
