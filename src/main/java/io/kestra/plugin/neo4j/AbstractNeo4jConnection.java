package io.kestra.plugin.neo4j;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;

@SuperBuilder
@NoArgsConstructor
@Getter
public abstract class AbstractNeo4jConnection extends Task implements Neo4jConnectionInterface {
    private Property<String> url;

    @Schema(
        title = "Username for basic auth",
        description = "Used with `password`; takes precedence over bearer tokens when both are set."
    )
    private Property<String> username;

    @Schema(
        title = "Password for basic auth",
        description = "Used with `username`; ignored if credentials are absent."
    )
    private Property<String> password;

    @Schema(
        title = "Bearer token",
        description = "Base64-encoded bearer token used when basic credentials are not provided."
    )
    private Property<String> bearerToken;

    protected AuthToken credentials(RunContext runContext) throws IllegalVariableEvaluationException {
        if (username != null && password != null) {
            return AuthTokens.basic(runContext.render(username).as(String.class).orElseThrow(), runContext.render(password).as(String.class).orElseThrow());
        }

        if (bearerToken != null) {
            return AuthTokens.bearer(runContext.render(bearerToken).as(String.class).orElseThrow());
        }

        return AuthTokens.none();
    }
}
