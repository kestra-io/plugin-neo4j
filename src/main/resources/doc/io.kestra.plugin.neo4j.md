# How to use the Neo4j plugin

Run Cypher queries, batch-load data, or run several statements in one transaction against Neo4j from Kestra flows.

## Authentication

Set `url` to your Neo4j endpoint (Bolt URI, e.g. `bolt://localhost:7687`, or HTTP(S)). For basic auth, set `username` and `password`. For token auth, set `bearerToken` (Base64-encoded). Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and set connection properties on each task.

## Tasks

`Query` runs a Cypher statement set in `query`. Control result handling with `storeType`: `NONE` (default, discards results), `FETCH` returns all rows, `FETCHONE` returns the first row, `STORE` writes results to internal storage. Bind values safely with `parameters` (e.g. `MATCH (p:Person {name: $name})` with `parameters: {name: "Alice"}`) instead of rendering them into the query string. Target a specific database with `database` (e.g. `analytics`); when empty, the server default database is used.

`Batch` bulk-loads data from a file in internal storage — set `from` to a `kestra://` URI and `query` to a Cypher `UNWIND $props AS ...` statement. Control batch size with `chunk` (default 1000). Like `Query`, it runs against `database` when set and the server default otherwise.

`Transaction` runs several Cypher statements atomically inside a single managed write transaction. Set `statements` to a list of `query`/`parameters` entries; they execute in declaration order and any failure rolls the whole transaction back. The driver retries transient failures such as deadlocks and leader switches for up to `maxRetryTime` (default `PT30S`); because a retry can replay an ambiguous commit, keep statements idempotent (`MERGE` instead of `CREATE`). The task returns the aggregated Neo4j write counters (`counters.nodesCreated`, `counters.relationshipsCreated`, `counters.propertiesSet`, ...) and emits one metric per counter. Like `Query`, it runs against `database` when set and the server default otherwise.

## Triggers

`Trigger` periodically executes a Cypher query and starts a flow execution when the query returns at least one row.

Key properties:

- `query` — Cypher query to execute.
- `storeType` — controls result handling: `FETCH` (default), `FETCHONE`, or `STORE`. `NONE` is unsupported for triggers and throws an error.
- `interval` — time between query executions, defaulting to 60 seconds.

Trigger outputs include:

- `trigger.rows` — returned rows.
- `trigger.row` — the first returned row.
- `trigger.uri` — internal storage URI when using `STORE`.
- `trigger.size` — number of returned rows.

The trigger does not keep state between polls. If the query continues to return rows, the trigger fires again on every polling interval. To avoid processing the same records repeatedly, use an idempotent query pattern, such as filtering on a processed flag and updating that flag after processing.

`storeType: NONE` is unsupported and throws an `IllegalArgumentException`.
