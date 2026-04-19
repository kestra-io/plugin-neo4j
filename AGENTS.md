# Kestra Neo4j Plugin

## What

- Provides plugin components under `io.kestra.plugin.neo4j`.
- Includes classes such as `Batch`, `Query`, `StoreType`.

## Why

- What user problem does this solve? Teams need to run Cypher queries against Neo4j from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Neo4j steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Neo4j.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `neo4j`

### Key Plugin Classes

- `io.kestra.plugin.neo4j.Batch`
- `io.kestra.plugin.neo4j.Query`

### Project Structure

```
plugin-neo4j/
├── src/main/java/io/kestra/plugin/neo4j/models/
├── src/test/java/io/kestra/plugin/neo4j/models/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
