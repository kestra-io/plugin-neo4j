package io.kestra.plugin.neo4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

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
import jakarta.validation.constraints.NotEmpty;
import lombok.AccessLevel;
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
                      - query: "MERGE (:Team {name: 'core'})"
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
        ),
        @Metric(
            name = "labels.removed",
            type = Counter.TYPE,
            description = "The number of labels removed from nodes by the transaction."
        )
    }
)
public class Transaction extends AbstractNeo4jConnection implements RunnableTask<Transaction.Output> {
    @JsonIgnore
    @Builder.Default
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private AtomicReference<Runnable> driverClose = new AtomicReference<>();

    @NotEmpty
    @Schema(
        title = "Cypher statements to run atomically",
        description = "Rendered with Flow variables, then executed in declaration order inside one managed write transaction. A failure in any statement rolls back all of them."
    )
    @PluginProperty(group = "main")
    private List<Statement> statements;

    @Builder.Default
    @Schema(
        title = "Maximum retry time",
        description = "ISO-8601 duration (default `PT30S`) during which the driver retries the transaction on transient failures such as deadlocks or leader switches."
    )
    @PluginProperty(group = "reliability")
    private Property<Duration> maxRetryTime = Property.ofValue(Duration.ofSeconds(30));

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        List<RenderedStatement> rStatements = this.renderStatements(runContext);

        Duration rMaxRetryTime = runContext.render(this.maxRetryTime).as(Duration.class).orElse(Duration.ofSeconds(30));
        if (rMaxRetryTime.isNegative()) {
            throw new IllegalArgumentException("The `maxRetryTime` property must not be negative, got " + rMaxRetryTime + ".");
        }

        Config config = Config.builder()
            .withMaxTransactionRetryTime(rMaxRetryTime.toMillis(), TimeUnit.MILLISECONDS)
            .build();

        String rUrl = runContext.render(getUrl()).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("The `url` property is required."));

        logger.debug("Executing {} statement(s) in a single managed transaction", rStatements.size());

        try (Driver driver = GraphDatabase.driver(rUrl, this.credentials(runContext), config); Session session = driver.session(this.sessionConfig(runContext))) {
            this.driverClose.set(driver::close);

            AtomicInteger failedStatement = new AtomicInteger(-1);

            Totals totals;
            try {
                totals = session.executeWrite(transaction -> {
                    Totals transactionTotals = new Totals();

                    for (int i = 0; i < rStatements.size(); i++) {
                        failedStatement.set(i);
                        RenderedStatement rStatement = rStatements.get(i);

                        ResultSummary summary = transaction.run(rStatement.query(), rStatement.parameters()).consume();
                        transactionTotals.add(summary.counters());
                        transactionTotals.resultAvailableAfterMillis += summary.resultAvailableAfter(TimeUnit.MILLISECONDS);
                    }

                    failedStatement.set(-1);
                    return transactionTotals;
                });
            } catch (Neo4jException e) {
                int index = failedStatement.get();
                if (index < 0) {
                    throw e;
                }

                throw new IllegalStateException("Statement " + (index + 1) + " failed, the transaction was rolled back: " + e.getMessage(), e);
            }

            runContext.metric(Counter.of("nodes.created", totals.nodesCreated, "origin", "Transaction"));
            runContext.metric(Counter.of("nodes.deleted", totals.nodesDeleted, "origin", "Transaction"));
            runContext.metric(Counter.of("relationships.created", totals.relationshipsCreated, "origin", "Transaction"));
            runContext.metric(Counter.of("relationships.deleted", totals.relationshipsDeleted, "origin", "Transaction"));
            runContext.metric(Counter.of("properties.set", totals.propertiesSet, "origin", "Transaction"));
            runContext.metric(Counter.of("labels.added", totals.labelsAdded, "origin", "Transaction"));
            runContext.metric(Counter.of("labels.removed", totals.labelsRemoved, "origin", "Transaction"));

            logger.info(
                "Committed {} statement(s): {} node(s) created, {} node(s) deleted, {} relationship(s) created, {} relationship(s) deleted, {} properties set",
                rStatements.size(),
                totals.nodesCreated,
                totals.nodesDeleted,
                totals.relationshipsCreated,
                totals.relationshipsDeleted,
                totals.propertiesSet
            );

            return Output.builder()
                .counters(
                    Counters.builder()
                        .nodesCreated(totals.nodesCreated)
                        .nodesDeleted(totals.nodesDeleted)
                        .relationshipsCreated(totals.relationshipsCreated)
                        .relationshipsDeleted(totals.relationshipsDeleted)
                        .propertiesSet(totals.propertiesSet)
                        .labelsAdded(totals.labelsAdded)
                        .labelsRemoved(totals.labelsRemoved)
                        .build()
                )
                .resultAvailableAfter(totals.resultAvailableAfterMillis)
                .build();
        } finally {
            this.driverClose.set(null);
        }
    }

    @Override
    public void kill() {
        Runnable close = this.driverClose.getAndSet(null);
        if (close == null) {
            return;
        }

        try {
            close.run();
        } catch (RuntimeException e) {
            LoggerFactory.getLogger(Transaction.class).warn("Failed to close the Neo4j driver after the task was killed", e);
        }
    }

    private List<RenderedStatement> renderStatements(RunContext runContext) throws Exception {
        if (this.statements == null || this.statements.isEmpty()) {
            throw new IllegalArgumentException("The `statements` property must contain at least one statement.");
        }

        List<RenderedStatement> rStatements = new ArrayList<>(this.statements.size());

        for (int i = 0; i < this.statements.size(); i++) {
            int position = i + 1;
            Statement statement = this.statements.get(i);
            if (statement == null || statement.getQuery() == null) {
                throw new IllegalArgumentException("Statement " + position + " must define a `query`.");
            }

            String rQuery = runContext.render(statement.getQuery()).as(String.class)
                .filter(query -> !query.isBlank())
                .orElseThrow(() -> new IllegalArgumentException("Statement " + position + " must define a non-blank `query`."));

            rStatements.add(new RenderedStatement(rQuery, this.renderParameters(runContext, statement)));
        }

        return rStatements;
    }

    private Map<String, Object> renderParameters(RunContext runContext, Statement statement) throws Exception {
        if (statement.getParameters() == null) {
            return Map.of();
        }

        Map<String, Object> rParameters = runContext.render(statement.getParameters()).asMap(String.class, Object.class);

        return rParameters == null ? Map.of() : rParameters;
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
        private long labelsRemoved;
        private long resultAvailableAfterMillis;

        private void add(SummaryCounters counters) {
            this.nodesCreated += counters.nodesCreated();
            this.nodesDeleted += counters.nodesDeleted();
            this.relationshipsCreated += counters.relationshipsCreated();
            this.relationshipsDeleted += counters.relationshipsDeleted();
            this.propertiesSet += counters.propertiesSet();
            this.labelsAdded += counters.labelsAdded();
            this.labelsRemoved += counters.labelsRemoved();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
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

        @Schema(title = "Labels removed")
        private final Long labelsRemoved;
    }
}
