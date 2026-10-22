package io.kestra.plugin.neo4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;
import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.neo4j.models.Statement;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@NoArgsConstructor
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@Schema(
    title = "Run multiple Cypher statements in one transaction",
    description = "Executes all `statements` inside a single managed write transaction, in order. If any statement fails the whole transaction is rolled back, and the driver retries transient failures (deadlocks, leader switches) until `maxRetryTime` is exhausted. Returns the aggregated Neo4j counters of the transaction."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Create a node and link it atomically",
            code = """
                id: neo4j_transaction
                namespace: company.team

                inputs:
                  - id: id
                    type: STRING
                  - id: name
                    type: STRING

                tasks:
                  - id: tx
                    type: io.kestra.plugin.neo4j.Transaction
                    url: "{{ secret('NEO4J_URL') }}"
                    username: "{{ secret('NEO4J_USER') }}"
                    password: "{{ secret('NEO4J_PASSWORD') }}"
                    statements:
                      - query: "CREATE (:Person {id: $id, name: $name})"
                        parameters:
                          id: "{{ inputs.id }}"
                          name: "{{ inputs.name }}"
                      - query: |
                          MATCH (p:Person {id: $id}), (t:Team {name: 'core'})
                          MERGE (p)-[:MEMBER_OF]->(t)
                        parameters:
                          id: "{{ inputs.id }}"
                """
        ),
        @Example(
            full = true,
            title = "Log the write summary of a transaction",
            code = """
                id: neo4j_transaction_summary
                namespace: company.team

                tasks:
                  - id: tx
                    type: io.kestra.plugin.neo4j.Transaction
                    url: "{{ secret('NEO4J_URL') }}"
                    username: "{{ secret('NEO4J_USER') }}"
                    password: "{{ secret('NEO4J_PASSWORD') }}"
                    statements:
                      - query: "MATCH (n:Temp) DETACH DELETE n"

                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "Deleted {{ outputs.tx.counters.nodesDeleted }} nodes"
                """
        )
    },
    metrics = {
        @Metric(
            name = "nodes.created",
            type = Counter.TYPE,
            description = "The number of nodes created by the transaction."
        ),
        @Metric(
            name = "nodes.deleted",
            type = Counter.TYPE,
            description = "The number of nodes deleted by the transaction."
        ),
        @Metric(
            name = "relationships.created",
            type = Counter.TYPE,
            description = "The number of relationships created by the transaction."
        ),
        @Metric(
            name = "relationships.deleted",
            type = Counter.TYPE,
            description = "The number of relationships deleted by the transaction."
        ),
        @Metric(
            name = "properties.set",
            type = Counter.TYPE,
            description = "The number of properties set by the transaction."
        ),
        @Metric(
            name = "labels.added",
            type = Counter.TYPE,
            description = "The number of labels added to nodes by the transaction."
        )
    }
)
public class Transaction extends AbstractNeo4jConnection implements RunnableTask<Transaction.Output> {
    private static final Duration DEFAULT_MAX_RETRY_TIME = Duration.ofSeconds(30);

    @NotNull
    @Schema(
        title = "Cypher statements to run atomically",
        description = "Rendered with Flow variables, then executed in declaration order inside one managed write transaction. A failure in any statement rolls back all of them."
    )
    @PluginProperty(group = "main")
    private Property<List<Statement>> statements;

    @Schema(
        title = "Maximum retry time",
        description = "ISO-8601 duration (default `PT30S`) during which the driver retries the transaction on transient failures such as deadlocks or leader switches."
    )
    @PluginProperty(group = "reliability")
    private Property<Duration> maxRetryTime;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        List<Statement> statements = runContext.render(this.statements).asList(Statement.class);
        if (statements == null || statements.isEmpty()) {
            throw new IllegalArgumentException("The `statements` property must contain at least one Cypher statement.");
        }

        List<RenderedStatement> rendered = this.renderStatements(runContext, statements);

        Duration maxRetry = runContext.render(this.maxRetryTime).as(Duration.class).orElse(DEFAULT_MAX_RETRY_TIME);

        Config config = Config.builder()
            .withMaxTransactionRetryTime(maxRetry.toMillis(), TimeUnit.MILLISECONDS)
            .build();

        logger.debug("Executing {} statement(s) in a single managed transaction", rendered.size());

        try (Driver driver = GraphDatabase.driver(runContext.render(getUrl()).as(String.class).orElseThrow(), this.credentials(runContext), config); Session session = driver.session(this.sessionConfig(runContext))) {
            Totals totals = session.executeWrite(transaction -> {
                Totals transactionTotals = new Totals();

                for (RenderedStatement statement : rendered) {
                    ResultSummary summary = transaction.run(statement.query(), statement.parameters()).consume();
                    transactionTotals.add(summary.counters());
                    transactionTotals.resultAvailableAfterMillis += summary.resultAvailableAfter(TimeUnit.MILLISECONDS);
                }

                return transactionTotals;
            });

            runContext.metric(Counter.of("nodes.created", totals.nodesCreated, "origin", "Transaction"));
            runContext.metric(Counter.of("nodes.deleted", totals.nodesDeleted, "origin", "Transaction"));
            runContext.metric(Counter.of("relationships.created", totals.relationshipsCreated, "origin", "Transaction"));
            runContext.metric(Counter.of("relationships.deleted", totals.relationshipsDeleted, "origin", "Transaction"));
            runContext.metric(Counter.of("properties.set", totals.propertiesSet, "origin", "Transaction"));
            runContext.metric(Counter.of("labels.added", totals.labelsAdded, "origin", "Transaction"));

            logger.info(
                "Committed {} statement(s): {} node(s) created, {} node(s) deleted, {} relationship(s) created, {} relationship(s) deleted, {} properties set",
                rendered.size(),
                totals.nodesCreated,
                totals.nodesDeleted,
                totals.relationshipsCreated,
                totals.relationshipsDeleted,
                totals.propertiesSet
            );

            return Output.builder()
                .statements((long) rendered.size())
                .counters(
                    Counters.builder()
                        .nodesCreated(totals.nodesCreated)
                        .nodesDeleted(totals.nodesDeleted)
                        .relationshipsCreated(totals.relationshipsCreated)
                        .relationshipsDeleted(totals.relationshipsDeleted)
                        .propertiesSet(totals.propertiesSet)
                        .labelsAdded(totals.labelsAdded)
                        .build()
                )
                .resultAvailableAfter(totals.resultAvailableAfterMillis)
                .build();
        }
    }

    private List<RenderedStatement> renderStatements(RunContext runContext, List<Statement> statements) throws Exception {
        List<RenderedStatement> rendered = new ArrayList<>(statements.size());

        for (Statement statement : statements) {
            if (statement == null || statement.getQuery() == null) {
                throw new IllegalArgumentException("Every entry of `statements` must define a `query`.");
            }

            rendered.add(new RenderedStatement(
                runContext.render(statement.getQuery()).as(String.class).orElseThrow(),
                this.renderParameters(runContext, statement)
            ));
        }

        return rendered;
    }

    private Map<String, Object> renderParameters(RunContext runContext, Statement statement) throws Exception {
        if (statement.getParameters() == null) {
            return Map.of();
        }

        Map<String, Object> parameters = runContext.render(statement.getParameters()).asMap(String.class, Object.class);

        return parameters == null ? Map.of() : parameters;
    }

    private record RenderedStatement(String query, Map<String, Object> parameters) {
    }

    private static final class Totals {
        private long nodesCreated;
        private long nodesDeleted;
        private long relationshipsCreated;
        private long relationshipsDeleted;
        private long propertiesSet;
        private long labelsAdded;
        private long resultAvailableAfterMillis;

        private void add(SummaryCounters counters) {
            this.nodesCreated += counters.nodesCreated();
            this.nodesDeleted += counters.nodesDeleted();
            this.relationshipsCreated += counters.relationshipsCreated();
            this.relationshipsDeleted += counters.relationshipsDeleted();
            this.propertiesSet += counters.propertiesSet();
            this.labelsAdded += counters.labelsAdded();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of statements executed",
            description = "All statements were committed together, or none were."
        )
        private final Long statements;

        @Schema(
            title = "Aggregated write counters",
            description = "Sum of the Neo4j counters of every statement of the transaction."
        )
        private final Counters counters;

        @Schema(
            title = "Time until results were available (ms)",
            description = "Total number of milliseconds the transaction took before the results of all statements were available."
        )
        private final Long resultAvailableAfter;
    }

    @Builder
    @Getter
    public static class Counters {
        @Schema(title = "Nodes created")
        private final Long nodesCreated;

        @Schema(title = "Nodes deleted")
        private final Long nodesDeleted;

        @Schema(title = "Relationships created")
        private final Long relationshipsCreated;

        @Schema(title = "Relationships deleted")
        private final Long relationshipsDeleted;

        @Schema(title = "Properties set")
        private final Long propertiesSet;

        @Schema(title = "Labels added")
        private final Long labelsAdded;
    }
}
