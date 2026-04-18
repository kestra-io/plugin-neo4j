# Kestra Neo4j Plugin

## What

- Provides plugin components under `io.kestra.plugin.neo4j`.
- Includes classes such as `Batch`, `Query`, `StoreType`.

## Why

- This plugin integrates Kestra with Neo4j.
- It provides tasks that run Cypher queries against Neo4j.

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
