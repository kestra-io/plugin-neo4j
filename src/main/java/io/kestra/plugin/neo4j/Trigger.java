package io.kestra.plugin.neo4j;

import java.time.Duration;
import java.util.Optional;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.neo4j.models.StoreType;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Poll Neo4j and trigger on query results",
    description = "Periodically executes a Cypher query and starts a flow execution when it returns at least one row. " +
        "The trigger keeps no state between polls and fires again on every interval while the query returns rows. " +
        "Make the query idempotent, for example by filtering on a processed flag and updating it after processing. " +
        "storeType: NONE never fires."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a flow when Neo4j returns rows",
            full = true,
            code = """
                id: neo4j_trigger
                namespace: company.team

                tasks:
                  - id: process_rows
                    type: io.kestra.plugin.core.debug.Return
                    format: "{{ trigger.size }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.neo4j.Trigger
                    url: "{{ secret('NEO4J_URL') }}"
                    username: "{{ secret('NEO4J_USERNAME') }}"
                    password: "{{ secret('NEO4J_PASSWORD') }}"
                    query: |
                      MATCH (p:Person)
                      RETURN p {.name} AS person
                    interval: PT1M
                """
        )
    }
)
public class Trigger extends AbstractTrigger
    implements PollingTriggerInterface, TriggerOutput<Query.Output>, Neo4jConnectionInterface {

    @Schema(title = "Neo4j endpoint URL")
    @PluginProperty(group = "connection")
    private Property<String> url;

    @Schema(title = "Username for basic authentication")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> username;

    @Schema(title = "Password for basic authentication")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> password;

    @Schema(title = "Bearer token")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> bearerToken;

    @Schema(title = "Cypher query to execute")
    @PluginProperty(group = "main")
    private Property<String> query;

    @Schema(title = "Result handling mode")
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<StoreType> storeType = Property.ofValue(StoreType.FETCH);

    @Schema(
        title = "Polling interval",
        description = "Time between query executions. Defaults to 60 seconds."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private final Duration interval = Duration.ofSeconds(60);

    protected Query createQuery() {
        return Query.builder()
            .id(this.id)
            .type(Query.class.getName())
            .url(this.url)
            .username(this.username)
            .password(this.password)
            .bearerToken(this.bearerToken)
            .query(this.query)
            .storeType(this.storeType)
            .build();
    }

    @Override
    public Optional<Execution> evaluate(
        ConditionContext conditionContext,
        TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        Query queryTask = this.createQuery();
        Query.Output output = queryTask.run(runContext);
        long size = Optional.ofNullable(output.getSize()).orElse(0L);

        logger.debug("Neo4j trigger query returned {} rows", size);

        if (size == 0) {
            if (output.getUri() != null) {
                runContext.storage().deleteFile(output.getUri());
            }
            return Optional.empty();
        }

        return Optional.of(
            TriggerService.generateExecution(
                this,
                conditionContext,
                context,
                output
            )
        );
    }
}