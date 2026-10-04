# How to use the Neo4j plugin

Run Cypher queries and batch-load data into Neo4j from Kestra flows.

## Authentication

Set `url` to your Neo4j endpoint (Bolt URI, e.g. `bolt://localhost:7687`, or HTTP(S)). For basic auth, set `username` and `password`. For token auth, set `bearerToken` (Base64-encoded). Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and set connection properties on each task.

## Tasks

`Query` runs a Cypher statement set in `query`. Control result handling with `storeType`: `NONE` (default, discards results), `FETCH` returns all rows, `FETCHONE` returns the first row, `STORE` writes results to internal storage. Bind values safely with `parameters` (e.g. `MATCH (p:Person {name: $name})` with `parameters: {name: "Alice"}`) instead of rendering them into the query string. Target a specific database with `database` (e.g. `analytics`); when empty, the server default database is used.

`Batch` bulk-loads data from a file in internal storage — set `from` to a `kestra://` URI and `query` to a Cypher `UNWIND $props AS ...` statement. Control batch size with `chunk` (default 1000). Like `Query`, it runs against `database` when set and the server default otherwise.
