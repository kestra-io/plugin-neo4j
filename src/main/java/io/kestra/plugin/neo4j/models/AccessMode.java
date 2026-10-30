package io.kestra.plugin.neo4j.models;

/**
 * Plugin-level representation of the Neo4j driver session access mode.
 * It is applied to every session through {@code SessionConfig}.
 */
public enum AccessMode {
    READ,
    WRITE
}
