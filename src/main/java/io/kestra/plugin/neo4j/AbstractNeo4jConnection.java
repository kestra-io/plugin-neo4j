package io.kestra.plugin.neo4j;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
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
    private String url;
    @Schema(
        title = "Username to use in case of basic auth",
        description = "If not specified, won't use basic"
    )
    @PluginProperty(dynamic = true)
    private String username;

    @Schema(
        title = "Password to use in case of basic auth",
        description = "If not specified, won't use basic auth"
    )
    @PluginProperty(dynamic = true)
    private String password;

    @Schema(
        title = "Token base64 encoded token"
    )
    @PluginProperty(dynamic = true)
    private String bearerToken;

    protected AuthToken credentials(RunContext runContext) throws IllegalVariableEvaluationException {
        if (username != null && password != null) {
            return AuthTokens.basic(runContext.render(username), runContext.render(password));
        }

        if (bearerToken != null) {
            return AuthTokens.bearer(runContext.render(bearerToken));
        }

        return AuthTokens.none();
    }
}
