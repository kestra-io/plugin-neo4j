package io.kestra.plugin.neo4j;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthToken;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Neo4jAuthTest {
    @Test
    void noneWhenNothingConfigured() {
        AuthToken token = AbstractNeo4jConnection.buildAuthToken(null, null, null, null, null, null, null);
        assertThat(token, notNullValue());
    }

    @Test
    void noneWhenOnlyBlanksConfigured() {
        AuthToken token = AbstractNeo4jConnection.buildAuthToken("  ", "", null, null, " ", null, null);
        assertThat(token, notNullValue());
    }

    @Test
    void basicToken() {
        AuthToken token = AbstractNeo4jConnection.buildAuthToken("neo4j", "secret", null, null, null, null, null);
        assertThat(token, notNullValue());
    }

    @Test
    void bearerToken() {
        AuthToken token = AbstractNeo4jConnection.buildAuthToken(null, null, "YmVhdG9rZW4=", null, null, null, null);
        assertThat(token, notNullValue());
    }

    @Test
    void kerberosToken() {
        AuthToken token = AbstractNeo4jConnection.buildAuthToken(null, null, null, "a2VyYmVyb3MtdGlja2V0", null, null, null);
        assertThat(token, notNullValue());
    }

    @Test
    void customToken() {
        AuthToken token = AbstractNeo4jConnection.buildAuthToken(null, null, null, null, "custom-scheme", "principal", "credentials");
        assertThat(token, notNullValue());
    }

    @Test
    void usernameWithoutPasswordIsRejected() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> AbstractNeo4jConnection.buildAuthToken("neo4j", null, null, null, null, null, null)
        );
        assertThat(e.getMessage(), containsString("username"));
        assertThat(e.getMessage(), containsString("password"));
    }

    @Test
    void passwordWithoutUsernameIsRejected() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> AbstractNeo4jConnection.buildAuthToken(null, "secret", null, null, null, null, null)
        );
        assertThat(e.getMessage(), containsString("username"));
        assertThat(e.getMessage(), containsString("password"));
    }

    @Test
    void basicAndBearerConflictIsRejected() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> AbstractNeo4jConnection.buildAuthToken("neo4j", "secret", "YmVhdG9rZW4=", null, null, null, null)
        );
        assertThat(e.getMessage(), containsString("conflicting"));
    }

    @Test
    void basicAndKerberosConflictIsRejected() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> AbstractNeo4jConnection.buildAuthToken("neo4j", "secret", null, "a2VyYmVyb3MtdGlja2V0", null, null, null)
        );
        assertThat(e.getMessage(), containsString("conflicting"));
    }

    @Test
    void bearerAndKerberosConflictIsRejected() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> AbstractNeo4jConnection.buildAuthToken(null, null, "YmVhdG9rZW4=", "a2VyYmVyb3MtdGlja2V0", null, null, null)
        );
        assertThat(e.getMessage(), containsString("conflicting"));
    }

    @Test
    void customAndBasicConflictIsRejected() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> AbstractNeo4jConnection.buildAuthToken("neo4j", "secret", null, null, "scheme", "principal", "credentials")
        );
        assertThat(e.getMessage(), containsString("conflicting"));
    }

    @Test
    void customAndBearerConflictIsRejected() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> AbstractNeo4jConnection.buildAuthToken(null, null, "YmVhdG9rZW4=", null, "scheme", "principal", "credentials")
        );
        assertThat(e.getMessage(), containsString("conflicting"));
    }

    @Test
    void customAndKerberosConflictIsRejected() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> AbstractNeo4jConnection.buildAuthToken(null, null, null, "a2VyYmVyb3MtdGlja2V0", "scheme", "principal", "credentials")
        );
        assertThat(e.getMessage(), containsString("conflicting"));
    }

    @Test
    void customWithoutSchemeIsRejected() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> AbstractNeo4jConnection.buildAuthToken(null, null, null, null, null, "principal", "credentials")
        );
        assertThat(e.getMessage(), containsString("customAuthScheme"));
    }

    @Test
    void customWithoutPrincipalIsRejected() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> AbstractNeo4jConnection.buildAuthToken(null, null, null, null, "scheme", null, "credentials")
        );
        assertThat(e.getMessage(), containsString("customAuthPrincipal"));
    }

    @Test
    void customWithoutCredentialsIsRejected() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> AbstractNeo4jConnection.buildAuthToken(null, null, null, null, "scheme", "principal", null)
        );
        assertThat(e.getMessage(), containsString("customAuthCredentials"));
    }
}
