# How to use the Neo4j plugin

Run Cypher queries and batch-load data into Neo4j from Kestra flows.

## Authentication

Set `url` to your Neo4j endpoint (Bolt URI, e.g. `bolt://localhost:7687`, or HTTP(S)). For basic auth, set `username` and `password`. For token auth, set `bearerToken` (Base64-encoded). Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and set connection properties on each task.

## Tasks

`Query` runs a Cypher statement set in `query`. Control result handling with `storeType`: `NONE` (default, discards results), `FETCH` returns all rows, `FETCHONE` returns the first row, `STORE` writes results to internal storage. Bind values safely with `parameters` (e.g. `MATCH (p:Person {name: $name})` with `parameters: {name: "Alice"}`) instead of rendering them into the query string. Target a specific database with `database` (e.g. `analytics`); when empty, the server default database is used.

`Batch` bulk-loads data from a file in internal storage — set `from` to a `kestra://` URI and `query` to a Cypher `UNWIND $props AS ...` statement. Control batch size with `chunk` (default 1000). Like `Query`, it runs against `database` when set and the server default otherwise.

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
