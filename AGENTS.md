# Kestra Neo4j Plugin

## What

Connect and interact with Neo4j graph databases in Kestra pipelines. Exposes 2 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Neo4j, allowing orchestration of Neo4j-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
