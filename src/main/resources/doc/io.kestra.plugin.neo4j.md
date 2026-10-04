# How to use the Neo4j plugin

Run Cypher queries and batch-load data into Neo4j from Kestra flows.

## Authentication

Set `url` to your Neo4j endpoint (Bolt URI, e.g. `bolt://localhost:7687`, or HTTP(S)). Exactly one authentication mode can be used at a time:

* Basic auth with `username` and `password` (both must be set together).
* Token auth with `bearerToken` (Base64-encoded).
* Kerberos with `kerberosTicket` (Base64-encoded service ticket).
* Custom auth with `customAuthScheme`, `customAuthPrincipal` and `customAuthCredentials` (all three required).
* No authentication (e.g. `NEO4J_AUTH=none` servers): leave every authentication property unset and `AuthTokens.none()` is used.

Conflicting modes are rejected with a validation error instead of silently picking one. Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and set connection properties on each task.

## TLS

Set `encryption: true` to force encrypted traffic. When `encryption` is unset, the driver default applies so URI schemes keep working (`bolt+s`, `bolt+ssc`, `neo4j+s`, `neo4j+ssc` stay encrypted, plain `bolt`/`neo4j` stay unencrypted).

Control server certificate trust with `trustStrategy`:

* `SYSTEM` (default) trusts system CA certificates.
* `CUSTOM` trusts the CA given in `trustedCertificate`.
* `ALL` trusts every certificate blindly — development and tests only.

`trustedCertificate` accepts either the PEM certificate content (e.g. `"{{ secret('NEO4J_CA_PEM') }}"`) or a Kestra internal storage URI (e.g. `kestra://.../ca.crt`). When `trustedCertificate` is supplied without an explicit `trustStrategy`, `CUSTOM` is inferred. Certificate content is never logged.

Tune `connectionTimeout` (default 30 seconds) and `maxConnectionPoolSize` (default 100) under the advanced group when needed.

## Tasks

`Query` runs a Cypher statement set in `query`. Control result handling with `storeType`: `NONE` (default, discards results), `FETCH` returns all rows, `FETCHONE` returns the first row, `STORE` writes results to internal storage. Use `accessMode: READ` to route sessions to read servers in a cluster (default `WRITE`).

`Batch` bulk-loads data from a file in internal storage — set `from` to a `kestra://` URI and `query` to a Cypher `UNWIND $props AS ...` statement. Control batch size with `chunk` (default 1000).
