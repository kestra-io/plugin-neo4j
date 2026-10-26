package io.kestra.plugin.neo4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.Config;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.summary.ResultSummary;
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
import jakarta.validation.constraints.NotNull;
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
    description = "Executes all `statements` inside a single managed write transaction, in order. A failure rolls the whole transaction back, and the driver retries transient failures (deadlocks, leader switches) until `maxRetryTime` is exhausted. A retry can replay an ambiguous commit, so keep statements idempotent (prefer `MERGE` over `CREATE`). Returns the aggregated Neo4j counters of the transaction."
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
                      - query: "MERGE (p:Person {id: $id}) SET p.name = $name"
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
                      - query: |
                          MERGE (r:CleanupRun {id: $id})
                          SET r.finishedAt = datetime()
                        parameters:
                          id: "{{ execution.id }}"

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
    private static final Duration DEFAULT_MAX_RETRY_TIME = Duration.ofSeconds(30);
    private static final Duration MAX_RETRY_TIME = Duration.ofMinutes(10);

    @JsonIgnore
    @Builder.Default
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicReference<Runnable> driverClose = new AtomicReference<>();

    @JsonIgnore
    @Builder.Default
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    @NotEmpty
    @Schema(
        title = "Cypher statements to run atomically",
        description = "Rendered with Flow variables, then executed in declaration order inside one managed write transaction. A failure in any statement rolls back all of them. The list itself is not rendered as one expression, so `statements: \"{{ outputs.x.list }}\"` does not resolve. Template values inside each `query` and `parameters` do."
    )
    @PluginProperty(group = "main")
    private List<@NotNull Statement> statements;

    @Builder.Default
    @Schema(
        title = "Maximum retry time",
        description = "ISO-8601 duration (default `PT30S`, maximum `PT10M`) during which the driver retries the transaction on transient failures such as deadlocks or leader switches. Must be between `PT0S` and `PT10M` inclusive."
    )
    @PluginProperty(group = "reliability")
    private Property<Duration> maxRetryTime = Property.ofValue(DEFAULT_MAX_RETRY_TIME);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var rStatements = this.renderStatements(runContext);

        var rMaxRetryTime = runContext.render(this.maxRetryTime).as(Duration.class).orElse(DEFAULT_MAX_RETRY_TIME);
        if (rMaxRetryTime.isNegative() || rMaxRetryTime.compareTo(MAX_RETRY_TIME) > 0) {
            throw new IllegalArgumentException(
                "The `maxRetryTime` property must be between PT0S and %s, got %s.".formatted(MAX_RETRY_TIME, rMaxRetryTime)
            );
        }

        var config = Config.builder()
            .withMaxTransactionRetryTime(rMaxRetryTime.toMillis(), TimeUnit.MILLISECONDS)
            .build();

        var rUrl = runContext.render(getUrl()).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("The `url` property is required."));

        logger.debug("Executing {} statement(s) in a single managed transaction", rStatements.size());

        try (var driver = GraphDatabase.driver(rUrl, this.credentials(runContext), config); var session = driver.session(this.sessionConfig(runContext))) {
            var closeDriver = (Runnable) driver::closeAsync;
            this.driverClose.set(closeDriver);
            if (this.isKilled.get()) {
                this.driverClose.compareAndSet(closeDriver, null);
                closeDriver.run();
                throw new IllegalStateException("The task was killed before the transaction started, so nothing was written.");
            }

            var failedStatement = new AtomicInteger(-1);

            Totals totals;
            try {
                // A retried commit may already be applied, so statements must be idempotent.
                totals = session.executeWrite(transaction -> {
                    var transactionTotals = new Totals();
                    failedStatement.set(-1);

                    for (var i = 0; i < rStatements.size(); i++) {
                        failedStatement.set(i);
                        var rStatement = rStatements.get(i);

                        var summary = transaction.run(rStatement.query(), rStatement.parameters()).consume();
                        transactionTotals.add(summary);
                    }

                    failedStatement.set(rStatements.size());
                    return transactionTotals;
                });
            } catch (RuntimeException e) {
                // A kill closes the driver, which can surface as a non-Neo4j exception from the retry loop
                if (this.isKilled.get()) {
                    throw new IllegalStateException(
                        "The task was killed and the driver was closed, so the transaction outcome is unknown. Check the database before rerunning: %s".formatted(e.getMessage()),
                        e
                    );
                }

                if (!(e instanceof Neo4jException)) {
                    throw e;
                }

                if (e instanceof ServiceUnavailableException || e instanceof SessionExpiredException) {
                    throw new IllegalStateException(
                        "The connection to Neo4j was lost, so the transaction outcome is unknown. Check the database before rerunning: %s".formatted(e.getMessage()),
                        e
                    );
                }

                var index = failedStatement.get();
                if (index < 0) {
                    throw new IllegalStateException(
                        "The transaction did not start, so nothing was written: %s. Check the connection settings and the database name.".formatted(e.getMessage()),
                        e
                    );
                }

                if (index >= rStatements.size()) {
                    throw new IllegalStateException("The commit failed and the transaction was rolled back: %s".formatted(e.getMessage()), e);
                }

                throw new IllegalStateException(
                    "Statement %d failed and the transaction was rolled back: %s. Check the query and parameters of statement %d.".formatted(index + 1, e.getMessage(), index + 1),
                    e
                );
            }

            totals.metrics(runContext);

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
                .counters(totals.toCounters())
                .resultAvailableAfter(totals.resultAvailableAfterMillis)
                .build();
        } finally {
            this.driverClose.set(null);
        }
    }

    @Override
    public void kill() {
        if (!this.isKilled.compareAndSet(false, true)) {
            return;
        }

        var close = this.driverClose.getAndSet(null);
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
        var rStatements = new ArrayList<RenderedStatement>();
        for (var statement : this.statements) {
            var position = rStatements.size() + 1;
            var rQuery = runContext.render(statement.getQuery()).as(String.class)
                .filter(query -> !query.isBlank())
                .orElseThrow(() -> new IllegalArgumentException("Statement %d must define a non-blank `query`.".formatted(position)));

            rStatements.add(new RenderedStatement(rQuery, runContext.render(statement.getParameters()).asMap(String.class, Object.class)));
        }

        return rStatements;
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

        private void add(ResultSummary summary) {
            var counters = summary.counters();
            this.nodesCreated += counters.nodesCreated();
            this.nodesDeleted += counters.nodesDeleted();
            this.relationshipsCreated += counters.relationshipsCreated();
            this.relationshipsDeleted += counters.relationshipsDeleted();
            this.propertiesSet += counters.propertiesSet();
            this.labelsAdded += counters.labelsAdded();
            this.labelsRemoved += counters.labelsRemoved();
            this.resultAvailableAfterMillis += summary.resultAvailableAfter(TimeUnit.MILLISECONDS);
        }

        private void metrics(RunContext runContext) {
            runContext.metric(Counter.of("nodes.created", this.nodesCreated, "origin", "Transaction"));
            runContext.metric(Counter.of("nodes.deleted", this.nodesDeleted, "origin", "Transaction"));
            runContext.metric(Counter.of("relationships.created", this.relationshipsCreated, "origin", "Transaction"));
            runContext.metric(Counter.of("relationships.deleted", this.relationshipsDeleted, "origin", "Transaction"));
            runContext.metric(Counter.of("properties.set", this.propertiesSet, "origin", "Transaction"));
            runContext.metric(Counter.of("labels.added", this.labelsAdded, "origin", "Transaction"));
            runContext.metric(Counter.of("labels.removed", this.labelsRemoved, "origin", "Transaction"));
        }

        private Counters toCounters() {
            return Counters.builder()
                .nodesCreated(this.nodesCreated)
                .nodesDeleted(this.nodesDeleted)
                .relationshipsCreated(this.relationshipsCreated)
                .relationshipsDeleted(this.relationshipsDeleted)
                .propertiesSet(this.propertiesSet)
                .labelsAdded(this.labelsAdded)
                .labelsRemoved(this.labelsRemoved)
                .build();
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
